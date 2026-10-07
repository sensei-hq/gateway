//! **Store conformance suite** for the orchestrator's persistence traits (torii move, TM-3,
//! gateway#79).
//!
//! The executor relies on properties of its stores that no type signature states: an
//! append-only journal folded in `Seq` order, content addressed by digest, a blackboard whose
//! collisions are loud, a scheduler whose transitions are conditional and whose wakes are claimed
//! exactly once, a drive lock that is exclusive, and a config generation that moves only with the
//! content. Each property is written down ONCE here, as a function over the trait object, and
//! every backend runs it: the gateway's in-memory stores here, and torii's tenant-scoped Postgres
//! stores in sensei-hq/torii (`docs/DECISIONS.md` §11: torii owns persistence). A backend that
//! passes is interchangeable with the others as far as the executor can tell.
//!
//! # Use
//!
//! Call each function from a `#[tokio::test]`, handing it a **fresh, isolated** store: one no
//! other test writes to concurrently, holding no rows (a new tenant, a truncated table, a new
//! in-memory value). Several operations are store-wide sweeps — `claim_due`, `list_paused`,
//! `prune_terminal`, the config generation — so a shared store gives false results either way.
//!
//! Each function panics, naming the clause that failed, on the first violation.
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_journal_keeps_the_contract() {
//!     orchestrator_testkit::journal(&MyJournal::new_for_test().await).await;
//! }
//! ```

use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{
    ConfigStore, ContentStore, ContextKey, ContextStore, ExecutionJournal, Graph, JournalEvent,
    NodeId, OrchestratorError, RegistryConfig, RunId, RunStatus, SchedulerStore, Scope, SkillDef,
    Snapshot, WakeAttempt, digest_of,
};

fn fresh_run() -> RunId {
    RunId(uuid::Uuid::new_v4())
}

/// Serialized form — the durable encoding is what a store must round-trip, and the event types
/// deliberately carry no `PartialEq`.
fn enc<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).expect("orchestrator types serialize")
}

fn started(version: &str) -> JournalEvent {
    JournalEvent::RunStarted {
        version: version.into(),
        budget: None,
        money_budget: None,
    }
}

fn node_started(node: &str) -> JournalEvent {
    JournalEvent::NodeStarted {
        node: NodeId(node.into()),
    }
}

// ---------------------------------------------------------------------------------------------
// ExecutionJournal
// ---------------------------------------------------------------------------------------------

/// [`ExecutionJournal`]: append-only, per-run, folded in `Seq` order; tail loads; latest-wins
/// snapshots; compaction removes exactly the named seqs and appends the manifest.
pub async fn journal(j: &dyn ExecutionJournal) {
    let a = fresh_run();
    let b = fresh_run();

    // An unknown run has an empty journal — not an error.
    assert!(
        j.load(a).await.expect("load an unknown run").is_empty(),
        "journal: an unknown run must load as empty"
    );

    let events = [started("v1"), node_started("n1"), node_started("n2")];
    let mut seqs = Vec::new();
    for e in &events {
        seqs.push(j.append(a, e.clone()).await.expect("append"));
    }
    let b_seq = j
        .append(b, node_started("other-run"))
        .await
        .expect("append");
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "journal: append must return strictly increasing Seq within a run: {seqs:?}"
    );

    let loaded = j.load(a).await.expect("load");
    assert_eq!(
        loaded.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        seqs,
        "journal: load returns every appended Seq, in order"
    );
    assert_eq!(
        loaded.iter().map(|(_, e)| enc(e)).collect::<Vec<_>>(),
        events.iter().map(enc).collect::<Vec<_>>(),
        "journal: load returns the events exactly as appended"
    );
    let b_loaded = j.load(b).await.expect("load other run");
    assert_eq!(
        b_loaded.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![b_seq],
        "journal: runs are isolated — another run's events never appear"
    );

    // Tail load: strictly greater than `since`.
    let tail = j.load_since(a, seqs[0]).await.expect("load_since");
    assert_eq!(
        tail.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        seqs[1..].to_vec(),
        "journal: load_since returns exactly the events with Seq > since"
    );
    assert!(
        j.load_since(a, seqs[2])
            .await
            .expect("load_since")
            .is_empty(),
        "journal: load_since past the last Seq is empty"
    );

    // Snapshots: none until written; latest wins; per run.
    assert!(
        j.latest_snapshot(a)
            .await
            .expect("latest_snapshot")
            .is_none(),
        "journal: no snapshot before one is written"
    );
    for (seq, node) in [(seqs[0], "n1"), (seqs[1], "n2")] {
        j.snapshot(
            a,
            Snapshot {
                seq,
                completed: vec![NodeId(node.into())],
                spent: 7,
                spent_micro_usd: 21_000,
                money_budget_micro_usd: Some(100_000),
                ..Default::default()
            },
        )
        .await
        .expect("snapshot");
    }
    let snap = j.latest_snapshot(a).await.expect("latest_snapshot").expect(
        "journal: a written snapshot must be returned (snapshots are part of the contract)",
    );
    assert_eq!(snap.seq, seqs[1], "journal: the LATEST snapshot wins");
    assert_eq!(
        enc(&snap.completed),
        enc(&vec![NodeId("n2".into())]),
        "journal: a snapshot round-trips its content"
    );
    assert_eq!(
        snap.spent, 7,
        "journal: a snapshot round-trips the spend ledger"
    );
    assert_eq!(
        (snap.spent_micro_usd, snap.money_budget_micro_usd),
        (21_000, Some(100_000)),
        "journal: a snapshot round-trips the MONEY half of the ledger (AG-12)"
    );
    assert!(
        j.latest_snapshot(b)
            .await
            .expect("latest_snapshot")
            .is_none(),
        "journal: snapshots are per run"
    );

    // AG-12: the money cap, a call's priced cost and a money raise round-trip EXACTLY — a
    // backend that drops an unknown jsonb key, or writes `null` where the field was absent,
    // either loses a run's dollar cap on resume or changes a money-free journal's bytes.
    let m = fresh_run();
    let money_events = [
        JournalEvent::RunStarted {
            version: "v1".into(),
            budget: None,
            money_budget: Some(orchestrator_core::MoneyBudget {
                total_micro_usd: 100_000,
            }),
        },
        JournalEvent::EffectRecorded {
            node: NodeId("n1".into()),
            effect_id: orchestrator_core::effect_id("n1", 0, 0),
            class: orchestrator_core::EffectClass::Pure,
            input_hash: "h".into(),
            seq: 0,
            output: orchestrator_core::EffectOutput::Inline(serde_json::json!("out")),
            observation: None,
            usage: Some(orchestrator_core::TokenUsage {
                input_tokens: 10,
                output_tokens: 100,
                total_tokens: 110,
                cost_micro_usd: Some(21_000),
            }),
        },
        JournalEvent::MoneyBudgetRaised {
            new_total_micro_usd: 1_000_000,
        },
    ];
    for e in &money_events {
        j.append(m, e.clone()).await.expect("append");
    }
    assert_eq!(
        j.load(m)
            .await
            .expect("load")
            .iter()
            .map(|(_, e)| enc(e))
            .collect::<Vec<_>>(),
        money_events.iter().map(enc).collect::<Vec<_>>(),
        "journal: the money cap, a call's cost and a money raise round-trip exactly (AG-12)"
    );

    // Compaction: remove exactly the named seqs, append the manifest after everything.
    let manifest = node_started("manifest");
    j.compact(a, &[seqs[1]], manifest.clone())
        .await
        .expect("compact");
    let after = j.load(a).await.expect("load after compact");
    let after_seqs: Vec<_> = after.iter().map(|(s, _)| *s).collect();
    assert_eq!(
        after_seqs.len(),
        3,
        "journal: compaction removes exactly the named seqs and appends one manifest: {after_seqs:?}"
    );
    assert_eq!(
        &after_seqs[..2],
        &[seqs[0], seqs[2]],
        "journal: compaction keeps every other event in order"
    );
    assert!(
        after_seqs[2] > seqs[2],
        "journal: the manifest is appended after the existing events"
    );
    assert_eq!(
        enc(&after[2].1),
        enc(&manifest),
        "journal: the manifest is the appended event"
    );
    assert_eq!(
        j.load(b).await.expect("load other run").len(),
        1,
        "journal: compaction never touches another run"
    );
}

// ---------------------------------------------------------------------------------------------
// ContentStore
// ---------------------------------------------------------------------------------------------

/// [`ContentStore`]: `put` is idempotent and content-addressed (the digest is `digest_of`);
/// `get` returns the bytes; a miss is a loud `ContentDigestMiss`, never empty bytes.
pub async fn content(c: &dyn ContentStore) {
    let bytes = br#"{"hello":"world"}"#;
    let d1 = c.put(bytes).await.expect("put");
    assert_eq!(
        d1,
        digest_of(bytes),
        "content: the digest is the content address (digest_of)"
    );
    let d2 = c.put(bytes).await.expect("put again");
    assert_eq!(d1, d2, "content: put is idempotent");
    assert_eq!(
        c.get(&d1).await.expect("get"),
        bytes.to_vec(),
        "content: get returns exactly the bytes put"
    );

    let other = b"different bytes";
    let d3 = c.put(other).await.expect("put other");
    assert_ne!(d1, d3, "content: different bytes, different digest");
    assert_eq!(c.get(&d3).await.expect("get other"), other.to_vec());

    let empty = c.put(b"").await.expect("put empty");
    assert_eq!(
        c.get(&empty).await.expect("get empty"),
        Vec::<u8>::new(),
        "content: empty content round-trips"
    );

    let miss = digest_of(b"never stored by this suite");
    match c.get(&miss).await {
        Err(OrchestratorError::ContentDigestMiss(_)) => {}
        other => panic!("content: a digest miss must be ContentDigestMiss, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// ContextStore
// ---------------------------------------------------------------------------------------------

/// [`ContextStore`]: keyed by `(run, scope, key)`; a repeat `put` is a loud collision; `get`
/// resolves `Node` → `Run` within the run and misses as `Ok(None)`; runs are isolated; `load`
/// returns the value; `insert_ref` is idempotent.
pub async fn context(x: &dyn ContextStore) {
    let a = fresh_run();
    let b = fresh_run();
    let key = |k: &str| ContextKey(k.into());
    let node = Scope::Node(NodeId("n1".into()));

    let r = x
        .put(a, Scope::Run, key("result"), serde_json::json!({"v": 1}))
        .await
        .expect("put");
    assert_eq!(
        x.load(&r).await.expect("load"),
        serde_json::json!({"v": 1}),
        "context: load returns the value put"
    );
    let got = x
        .get(a, Scope::Run, key("result"))
        .await
        .expect("get")
        .expect("context: a put entry is found by get");
    assert_eq!(got, r, "context: get returns the ref put returned");

    match x
        .put(a, Scope::Run, key("result"), serde_json::json!({"v": 2}))
        .await
    {
        Err(OrchestratorError::ContextKeyCollision { .. }) => {}
        other => panic!("context: a repeat put of (run, scope, key) must collide, got {other:?}"),
    }
    assert_eq!(
        x.load(&x.get(a, Scope::Run, key("result")).await.unwrap().unwrap())
            .await
            .unwrap(),
        serde_json::json!({"v": 1}),
        "context: a refused collision leaves the original value"
    );

    // Same key in another run: no collision, and invisible across runs.
    x.put(b, Scope::Run, key("result"), serde_json::json!({"v": "b"}))
        .await
        .expect("context: the same key in ANOTHER run must not collide");
    assert_eq!(
        x.load(&x.get(b, Scope::Run, key("result")).await.unwrap().unwrap())
            .await
            .unwrap(),
        serde_json::json!({"v": "b"}),
        "context: runs are isolated"
    );

    // Node scope resolves up to Run; a Node-scoped entry shadows the Run one.
    let up = x
        .get(a, node.clone(), key("result"))
        .await
        .expect("get")
        .expect("context: a Node-scoped read resolves up to the Run scope");
    assert_eq!(up, r);
    let local = x
        .put(
            a,
            node.clone(),
            key("result"),
            serde_json::json!({"v": "node"}),
        )
        .await
        .expect("context: a Node-scoped put does not collide with the Run-scoped key");
    assert_eq!(
        x.get(a, node.clone(), key("result")).await.unwrap(),
        Some(local),
        "context: the Node-scoped entry is preferred over the Run one"
    );
    assert_eq!(
        x.get(a, Scope::Run, key("result")).await.unwrap(),
        Some(r.clone()),
        "context: a Run-scoped read never sees a Node-scoped entry"
    );

    assert_eq!(
        x.get(a, Scope::Run, key("absent")).await.expect("get miss"),
        None,
        "context: a miss is Ok(None)"
    );
    // The Node → Run fallback stays inside the run (SP-OPS-1.1): runs a and b both hold a
    // Run-scoped "result", and a run that has none must not see theirs.
    let empty = fresh_run();
    assert_eq!(
        x.get(empty, node.clone(), key("result"))
            .await
            .expect("get"),
        None,
        "context: a Node-scoped read falls back only to its OWN run's Run scope"
    );
    assert_eq!(
        x.get(empty, Scope::Run, key("result")).await.expect("get"),
        None,
        "context: a Run-scoped read never crosses runs"
    );

    // insert_ref: rehydrate a journaled ref, idempotently, without a collision.
    let c = fresh_run();
    x.insert_ref(c, r.clone()).await.expect("insert_ref");
    x.insert_ref(c, r.clone())
        .await
        .expect("context: insert_ref must be idempotent (a fold replays every write)");
    assert_eq!(
        x.get(c, Scope::Run, key("result")).await.unwrap(),
        Some(r),
        "context: an inserted ref is found by get"
    );
}

// ---------------------------------------------------------------------------------------------
// SchedulerStore + RunLock
// ---------------------------------------------------------------------------------------------

/// Whole seconds, so a store with microsecond timestamps (Postgres) round-trips them exactly.
fn base_time() -> DateTime<Utc> {
    DateTime::from_timestamp(Utc::now().timestamp(), 0).expect("valid time")
}

fn empty_graph() -> Graph {
    Graph { nodes: vec![] }
}

async fn status_of(s: &dyn SchedulerStore, run: RunId) -> Option<RunStatus> {
    s.status(run).await.expect("status").map(|r| r.status)
}

/// [`SchedulerStore`] + [`RunLock`](orchestrator_core::RunLock), including AG-3's wake-attempt
/// counting and backoff (`begin_wake_attempt` / `record_wake_failed`). **Needs a fresh store with
/// no rows** — `claim_due`, `list_paused` and pruning are store-wide.
pub async fn scheduler(s: &dyn SchedulerStore) {
    let t0 = base_time();
    let lease = Duration::seconds(60);

    // enqueue → waking; duplicate → loud.
    let r1 = fresh_run();
    s.enqueue(r1, &empty_graph(), t0).await.expect("enqueue");
    assert_eq!(
        status_of(s, r1).await,
        Some(RunStatus::Waking),
        "scheduler: enqueue → waking"
    );
    assert!(
        s.enqueue(r1, &empty_graph(), t0).await.is_err(),
        "scheduler: a duplicate enqueue is an error"
    );
    assert_eq!(
        status_of(s, fresh_run()).await,
        None,
        "scheduler: unknown run → None"
    );

    // A fresh waking run is NOT claimed; a stale one is (crash-mid-wake reclaim).
    assert!(
        s.claim_due(t0 + Duration::seconds(30), lease, 10)
            .await
            .expect("claim")
            .is_empty(),
        "scheduler: a waking run inside its lease is not reclaimed"
    );
    let reclaimed = s
        .claim_due(t0 + Duration::seconds(61), lease, 10)
        .await
        .expect("claim");
    assert_eq!(
        reclaimed.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![r1],
        "scheduler: a waking run past its lease is reclaimed"
    );
    assert_eq!(
        enc(&reclaimed[0].1),
        enc(&empty_graph()),
        "scheduler: a claim returns the run's original graph"
    );

    // record_paused: waking → paused; conditional on waking.
    let t1 = t0 + Duration::seconds(100);
    s.record_paused(r1, Some(t1 + Duration::seconds(10)), "quota")
        .await
        .expect("record_paused");
    let st = s.status(r1).await.unwrap().unwrap();
    assert_eq!(
        st.status,
        RunStatus::Paused,
        "scheduler: record_paused → paused"
    );
    assert_eq!(
        st.reason.as_deref(),
        Some("quota"),
        "scheduler: the pause reason is kept"
    );
    assert_eq!(
        st.next_wake,
        Some(t1 + Duration::seconds(10)),
        "scheduler: the next wake is kept"
    );

    // A future wake is not claimed; a due one is, exactly once.
    assert!(
        s.claim_due(t1, lease, 10).await.unwrap().is_empty(),
        "scheduler: a paused run before its wake is not claimed"
    );
    let due = s
        .claim_due(t1 + Duration::seconds(10), lease, 10)
        .await
        .unwrap();
    assert_eq!(
        due.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![r1],
        "scheduler: a paused run at its wake is claimed"
    );
    assert_eq!(
        status_of(s, r1).await,
        Some(RunStatus::Waking),
        "scheduler: a claim → waking"
    );
    assert!(
        s.claim_due(t1 + Duration::seconds(10), lease, 10)
            .await
            .unwrap()
            .is_empty(),
        "scheduler: a claimed wake is not claimed twice"
    );

    // record_terminal: waking → completed; conditional on waking.
    s.record_terminal(r1, RunStatus::Completed, None)
        .await
        .expect("record_terminal");
    assert_eq!(status_of(s, r1).await, Some(RunStatus::Completed));
    s.record_paused(r1, Some(t1), "late")
        .await
        .expect("record_paused on a terminal row");
    assert_eq!(
        status_of(s, r1).await,
        Some(RunStatus::Completed),
        "scheduler: record_paused is a no-op unless the row is waking"
    );

    // A NULL-deadline pause is never claimed by the timer; force_wake makes it claimable.
    let r2 = fresh_run();
    s.enqueue(r2, &empty_graph(), t1).await.unwrap();
    s.record_paused(r2, None, "awaiting a human").await.unwrap();
    assert!(
        s.claim_due(t1 + Duration::days(365), lease, 10)
            .await
            .unwrap()
            .is_empty(),
        "scheduler: a NULL-deadline pause is never claimed by the timer"
    );
    let paused: Vec<_> = s
        .list_paused()
        .await
        .unwrap()
        .iter()
        .map(|r| r.run)
        .collect();
    assert_eq!(
        paused,
        vec![r2],
        "scheduler: list_paused lists exactly the paused runs"
    );
    let t2 = t1 + Duration::seconds(200);
    s.force_wake(r2, t2).await.expect("force_wake");
    assert_eq!(
        s.claim_due(t2, lease, 10)
            .await
            .unwrap()
            .iter()
            .map(|(r, _)| *r)
            .collect::<Vec<_>>(),
        vec![r2],
        "scheduler: force_wake makes a NULL-deadline pause claimable"
    );
    s.force_wake(r2, t2).await.expect("force_wake on waking");
    assert_eq!(
        status_of(s, r2).await,
        Some(RunStatus::Waking),
        "scheduler: force_wake is a no-op unless paused"
    );

    // cancel: wins over a later transition, is idempotent, and is never woken.
    s.record_paused(r2, Some(t2), "again").await.unwrap();
    s.cancel(r2).await.expect("cancel");
    s.cancel(r2).await.expect("scheduler: cancel is idempotent");
    assert_eq!(status_of(s, r2).await, Some(RunStatus::Cancelled));
    assert!(
        s.claim_due(t2 + Duration::days(365), lease, 10)
            .await
            .unwrap()
            .is_empty(),
        "scheduler: a cancelled run is never claimed"
    );
    s.record_terminal(r2, RunStatus::Failed, Some("late"))
        .await
        .unwrap();
    assert_eq!(
        status_of(s, r2).await,
        Some(RunStatus::Cancelled),
        "scheduler: a cancelled row is never resurrected by a later transition"
    );
    s.cancel(r1).await.unwrap();
    assert_eq!(
        status_of(s, r1).await,
        Some(RunStatus::Completed),
        "scheduler: cancel never rewrites a terminal row"
    );
    s.force_wake(r1, t2)
        .await
        .expect("force_wake on a terminal row");
    let st = s.status(r1).await.unwrap().unwrap();
    assert_eq!(
        (st.status, st.next_wake),
        (RunStatus::Completed, None),
        "scheduler: force_wake touches only a paused row (no wake is set on any other)"
    );

    // claim_due honours its limit.
    let many: Vec<RunId> = (0..3).map(|_| fresh_run()).collect();
    for r in &many {
        s.enqueue(*r, &empty_graph(), t2).await.unwrap();
        s.record_paused(*r, Some(t2), "batch").await.unwrap();
    }
    assert_eq!(
        s.claim_due(t2, lease, 2).await.unwrap().len(),
        2,
        "scheduler: claim_due claims at most `limit`"
    );
    assert_eq!(s.claim_due(t2, lease, 10).await.unwrap().len(), 1);

    // Pruning: terminal rows only, by allowlist; never a paused or waking row at any age.
    let keep_paused = fresh_run();
    s.enqueue(keep_paused, &empty_graph(), t0).await.unwrap();
    s.record_paused(keep_paused, None, "indefinite")
        .await
        .unwrap();
    // r1 (completed), r2 (cancelled) are terminal; `many` are waking; keep_paused is paused.
    let paused: Vec<_> = s
        .list_paused()
        .await
        .unwrap()
        .iter()
        .map(|r| r.run)
        .collect();
    assert_eq!(
        paused,
        vec![keep_paused],
        "scheduler: list_paused lists exactly the paused runs — not waking, cancelled or completed ones"
    );
    let far_past = DateTime::from_timestamp(0, 0).unwrap();
    assert_eq!(
        s.count_terminal_before(far_past).await.unwrap(),
        0,
        "scheduler: nothing is older than the epoch"
    );
    let far_future = Utc::now() + Duration::days(3650);
    let preview = s.count_terminal_before(far_future).await.unwrap();
    assert_eq!(
        preview, 2,
        "scheduler: count_terminal_before counts exactly the terminal rows"
    );
    let pruned = s.prune_terminal(far_future).await.unwrap();
    assert_eq!(
        pruned, preview,
        "scheduler: the prune deletes exactly what the preview counted"
    );
    assert_eq!(
        status_of(s, r1).await,
        None,
        "scheduler: a pruned row is gone"
    );
    assert_eq!(
        status_of(s, r2).await,
        None,
        "scheduler: every terminal status is prunable"
    );
    assert_eq!(
        status_of(s, keep_paused).await,
        Some(RunStatus::Paused),
        "scheduler: a paused run is never pruned, however old"
    );
    for r in &many {
        assert_eq!(
            status_of(s, *r).await,
            Some(RunStatus::Waking),
            "scheduler: a waking run is never pruned, however old"
        );
    }

    // The drive lock: exclusive while held; released by release() AND by drop.
    let locked = fresh_run();
    let held = s
        .try_lock_run(locked)
        .await
        .expect("try_lock_run")
        .expect("scheduler: an uncontended run lock is granted");
    assert!(
        s.try_lock_run(locked)
            .await
            .expect("try_lock_run")
            .is_none(),
        "scheduler: a held run lock is NOT granted again"
    );
    let other = s
        .try_lock_run(fresh_run())
        .await
        .expect("try_lock_run")
        .expect("scheduler: locks are per run");
    other.release().await.expect("release");
    held.release().await.expect("release");
    let again = s
        .try_lock_run(locked)
        .await
        .expect("try_lock_run")
        .expect("scheduler: a released lock can be taken again");
    drop(again);
    assert!(
        s.try_lock_run(locked)
            .await
            .expect("try_lock_run")
            .is_some(),
        "scheduler: dropping a lock without release() must also release it"
    );

    wake_attempts(s, t2 + Duration::days(30), lease).await;
}

fn claimed_runs(v: Vec<(RunId, Graph)>) -> Vec<RunId> {
    v.into_iter().map(|(r, _)| r).collect()
}

/// A retry schedule that ignores the attempt number — the deadline is fixed by the clause.
fn at(t: DateTime<Utc>) -> impl Fn(u32) -> DateTime<Utc> + Send + Sync {
    move |_| t
}

fn attempt(n: u32, last_error: Option<&str>) -> Option<WakeAttempt> {
    Some(WakeAttempt {
        attempt: n,
        last_error: last_error.map(str::to_string),
    })
}

/// AG-3: consecutive wake attempts are counted, a failed wake is backed off to the deadline the
/// driver chose, a lost drive is reclaimed no sooner than its armed retry, and a successful drive
/// resets the count. The driver's `max_attempts` cap is only as exact as this count, so the count
/// is asserted exactly. Runs at `ta`, long after every earlier row, and asserts claims by
/// membership — the rows the earlier clauses left `waking` are legitimately reclaimable here.
async fn wake_attempts(s: &dyn SchedulerStore, ta: DateTime<Utc>, lease: Duration) {
    let secs = Duration::seconds;
    let w = fresh_run();
    assert_eq!(
        s.begin_wake_attempt(w, &at(ta))
            .await
            .expect("begin_wake_attempt"),
        None,
        "scheduler: begin_wake_attempt on an unknown run → None"
    );
    s.enqueue(w, &empty_graph(), ta).await.unwrap();
    s.record_paused(w, Some(ta + secs(10)), "gated")
        .await
        .unwrap();
    assert_eq!(
        s.begin_wake_attempt(w, &at(ta + secs(10))).await.unwrap(),
        None,
        "scheduler: begin_wake_attempt is conditional on waking (a paused row is not a wake)"
    );
    assert!(claimed_runs(s.claim_due(ta + secs(10), lease, 100).await.unwrap()).contains(&w));
    assert_eq!(
        s.begin_wake_attempt(w, &at(ta + secs(40))).await.unwrap(),
        attempt(1, None),
        "scheduler: the first wake after a successful drive is attempt 1, with no prior error"
    );

    // A failed wake is backed off to EXACTLY the retry deadline the driver chose.
    let r1 = ta + secs(40);
    s.record_wake_failed(w, r1, "boom 1")
        .await
        .expect("record_wake_failed");
    let st = s.status(w).await.unwrap().unwrap();
    assert_eq!(
        (st.status, st.next_wake, st.reason.as_deref()),
        (RunStatus::Paused, Some(r1), Some("boom 1")),
        "scheduler: record_wake_failed → paused at the retry deadline, reason = the error"
    );
    assert!(
        !claimed_runs(s.claim_due(r1 - secs(1), lease, 100).await.unwrap()).contains(&w),
        "scheduler: a backed-off wake is not claimed before its retry deadline"
    );
    assert!(
        claimed_runs(s.claim_due(r1, lease, 100).await.unwrap()).contains(&w),
        "scheduler: a backed-off wake is claimed at its retry deadline"
    );
    let r2 = r1 + secs(120);
    assert_eq!(
        s.begin_wake_attempt(w, &at(r2)).await.unwrap(),
        attempt(2, Some("boom 1")),
        "scheduler: a failed attempt is counted and its error handed to the next attempt"
    );
    s.record_wake_failed(w, r2, "boom 2").await.unwrap();
    assert!(
        !claimed_runs(s.claim_due(r2 - secs(1), lease, 100).await.unwrap()).contains(&w),
        "scheduler: the second backoff is honoured too"
    );
    assert!(claimed_runs(s.claim_due(r2, lease, 100).await.unwrap()).contains(&w));

    // A LOST drive (no record at all): reclaimed only past BOTH its lease and its armed retry.
    let r3 = r2 + secs(240);
    // The schedule is keyed off the attempt number: 80s per attempt, so attempt 3 arms r3.
    let per_attempt = move |n: u32| r2 + secs(80 * i64::from(n));
    assert_eq!(
        s.begin_wake_attempt(w, &per_attempt).await.unwrap(),
        attempt(3, Some("boom 2"))
    );
    assert_eq!(
        s.status(w).await.unwrap().unwrap().next_wake,
        Some(r3),
        "scheduler: begin_wake_attempt arms next_wake = the schedule applied to the NEW attempt \
         number"
    );
    assert!(
        !claimed_runs(s.claim_due(r2 + lease + secs(1), lease, 100).await.unwrap()).contains(&w),
        "scheduler: a lost drive past its lease but before its armed retry is not reclaimed"
    );
    assert!(
        claimed_runs(s.claim_due(r3, lease, 100).await.unwrap()).contains(&w),
        "scheduler: a lost drive is reclaimed at its armed retry"
    );
    let r4 = r3 + secs(480);
    assert_eq!(
        s.begin_wake_attempt(w, &at(r4)).await.unwrap(),
        attempt(4, None),
        "scheduler: a lost attempt is counted, and the error it never recorded is not invented \
         (the previous error was taken by the attempt that saw it)"
    );

    // force_wake is an operator's "wake now": it skips the backoff, NOT the count.
    s.record_wake_failed(w, r4, "boom 4").await.unwrap();
    s.force_wake(w, r3 + secs(10)).await.unwrap();
    assert!(
        claimed_runs(s.claim_due(r3 + secs(10), lease, 100).await.unwrap()).contains(&w),
        "scheduler: force_wake makes a backed-off wake due now"
    );
    assert_eq!(
        s.begin_wake_attempt(w, &at(r4)).await.unwrap(),
        attempt(5, Some("boom 4")),
        "scheduler: force_wake does not reset the attempt count"
    );

    // A successful drive resets the count.
    s.record_paused(w, Some(r4), "quota").await.unwrap();
    assert!(claimed_runs(s.claim_due(r4, lease, 100).await.unwrap()).contains(&w));
    assert_eq!(
        s.begin_wake_attempt(w, &at(r4 + secs(30))).await.unwrap(),
        attempt(1, None),
        "scheduler: a successful drive (record_paused) resets the attempt count and the error"
    );

    // Conditional on waking: cancel wins; a terminal row is never touched.
    s.cancel(w).await.unwrap();
    s.record_wake_failed(w, r4 + secs(30), "late")
        .await
        .unwrap();
    let st = s.status(w).await.unwrap().unwrap();
    assert_eq!(
        (st.status, st.next_wake),
        (RunStatus::Cancelled, None),
        "scheduler: record_wake_failed never resurrects a cancelled run"
    );
    assert_eq!(
        s.begin_wake_attempt(w, &at(r4 + secs(60))).await.unwrap(),
        None,
        "scheduler: begin_wake_attempt on a terminal row → None"
    );

    // enqueue counts submit's inline drive as attempt 1, so a submit lost mid-drive is counted.
    let x = fresh_run();
    let tx = r4 + secs(1000);
    s.enqueue(x, &empty_graph(), tx).await.unwrap();
    assert!(
        claimed_runs(s.claim_due(tx + lease + secs(1), lease, 100).await.unwrap()).contains(&x),
        "scheduler: a submit lost mid-drive (NULL next_wake) is reclaimed after its lease"
    );
    assert_eq!(
        s.begin_wake_attempt(x, &at(tx + lease + secs(31)))
            .await
            .unwrap(),
        attempt(2, None),
        "scheduler: enqueue starts the count at 1 — submit's drive was the first attempt"
    );
}

// ---------------------------------------------------------------------------------------------
// ConfigStore
// ---------------------------------------------------------------------------------------------

fn cfg_with_skill(name: &str) -> RegistryConfig {
    RegistryConfig {
        skills: vec![SkillDef {
            name: name.into(),
            description: None,
            body: format!("body of {name}"),
            activation: Default::default(),
        }],
        ..Default::default()
    }
}

fn skill_names(cfg: &RegistryConfig) -> Vec<String> {
    cfg.skills.iter().map(|s| s.name.clone()).collect()
}

/// [`ConfigStore`] (TM-2): versioned (`Some(0)` before the first write), content and generation
/// move together, a stale compare-and-swap writes nothing. **Needs a store at generation 0.**
pub async fn config_store(store: &dyn ConfigStore) {
    assert_eq!(
        store.version().await.unwrap(),
        Some(0),
        "config: a fresh store is at Some(0)"
    );
    assert_eq!(store.load_versioned().await.unwrap().1, Some(0));

    let v1 = store
        .store_and_bump_if(&cfg_with_skill("first"), 0)
        .await
        .unwrap();
    assert_eq!(
        v1,
        Some(1),
        "config: a first push is a CAS at 0 and lands as 1"
    );
    let (cfg, v) = store.load_versioned().await.unwrap();
    assert_eq!((skill_names(&cfg), v), (vec!["first".to_string()], Some(1)));

    let stale = store
        .store_and_bump_if(&cfg_with_skill("stale"), 0)
        .await
        .unwrap();
    assert_eq!(stale, None, "config: a stale CAS is refused");
    let (cfg, v) = store.load_versioned().await.unwrap();
    assert_eq!(
        (skill_names(&cfg), v),
        (vec!["first".to_string()], Some(1)),
        "config: a refused CAS writes nothing"
    );

    let v2 = store
        .store_and_bump(&cfg_with_skill("second"))
        .await
        .unwrap();
    assert_eq!(v2, 2, "config: an unconditional write advances by one");
    let (cfg, v) = store.load_versioned().await.unwrap();
    assert_eq!(
        (skill_names(&cfg), v),
        (vec!["second".to_string()], Some(2)),
        "config: a write replaces the whole registry"
    );
    assert_eq!(skill_names(&store.load().await.unwrap()), ["second"]);

    let v3 = store
        .store_and_bump_if(&cfg_with_skill("third"), 2)
        .await
        .unwrap();
    assert_eq!(v3, Some(3), "config: a current CAS lands");
    assert_eq!(store.version().await.unwrap(), Some(3));
}
