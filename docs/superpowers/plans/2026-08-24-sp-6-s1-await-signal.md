# SP-6 s1 — `AwaitSignal` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A graph node that pauses until an external signal arrives, with an optional deadline that fails the node — the answer journaled and folded so a resumed run never re-asks.

**Architecture:** Two node-keyed journal events (`SignalAwaited`, `SignalReceived`) folded into two `HashMap<NodeId, _>` on `Fold`, exactly as `PlannerSelected` → `fold.selections` already works. The node itself is a three-way fold read. The **absolute deadline is journaled, never recomputed** — that is the slice's one real trap.

**Tech Stack:** Rust 2024, `serde` with `#[serde(default)]` for additive journal fields, `chrono`, existing `orchestrator-core`/`orchestrator`/`torii` crates, Docker `postgres:16`.

**Spec:** `docs/superpowers/specs/2026-08-24-sp-6-s1-await-signal-design.md`

**Baseline that must not regress:** `cargo test --workspace` = **1340 passed / 0 failed**, green with AND without `DATABASE_URL` at default parallelism; `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` both exit 0.

**Database:** start it before Task 4:
```bash
docker run -d --name torii-pg -e POSTGRES_HOST_AUTH_METHOD=trust -p 5433:5432 postgres:16
until docker exec torii-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
docker exec -i torii-pg psql -U postgres -q -v ON_ERROR_STOP=1 < database/_apply_all.sql
```
`DATABASE_URL=postgres://postgres@localhost:5433/postgres` — **env does not persist between shell calls; prefix every command.** DB tests skip vacuously without it and print `SKIP <test>` to fd 2.

---

## Verified facts every task depends on

Confirmed against the source; do not re-investigate:

- `fold_journal`'s `PlannerSelected` arm is `crates/orchestrator/src/executor/support.rs:172`:
  ```rust
  JournalEvent::PlannerSelected { node, agent } => {
      fold.selections.insert(node.clone(), agent.clone());
  }
  ```
  That is the exact shape both new arms take.
- `Fold` (private, `crates/orchestrator/src/executor/mod.rs`) has `selections: HashMap<NodeId, AgentRef>` at `:157`.
- `run_node`'s `NodeKind` match is `mod.rs:896-1038`; delegating arms look like
  `NodeKind::Loop { .. } => self.run_loop(run, node, fold).await,`.
- `NodeExec { Completed(Value), Failed { message, output }, Paused { reason } }` — `mod.rs`.
- `JournalEvent::RunPaused { reason, resume_after: Option<DateTime<Utc>> }` — **not node-keyed**, which is why `SignalAwaited` must exist separately.
- `self.clock.now()` is how the executor reads time (injected `Clock`; `FakeClock` in `test_support`).
- `pub const FORMAT_VERSION: i32 = 1;` — `journal.rs:15`.
- SP-DATA-5 established the convention that `fold_journal` gets **explicit** arms, never the `_` catch-all, for events whose loss would be silent.

---

## File structure

```
orchestrator-core
  src/journal.rs   +2 events (SignalAwaited, SignalReceived)
  src/graph.rs     +1 NodeKind variant (AwaitSignal)

orchestrator
  src/executor/mod.rs      Fold += signals, deadlines; run_node arm; run_await_signal
  src/executor/support.rs  fold_journal += 2 explicit arms

torii
  src/cmd/run.rs   signal(); list_paused shows the awaiting node + deadline
  src/main.rs      clap: run signal <run> --node <node> --payload <json>
  tests/e2e_pg.rs  AC7
```

---

## Task 1: the two events, the node variant, and restoring compilation

**Files:** `crates/orchestrator-core/src/journal.rs`, `crates/orchestrator-core/src/graph.rs`, **plus every construction and match site across the workspace needed to compile again.**

**This task is wider than it looks, and that is deliberate — a lesson from SP-DATA-5.** Adding a `NodeKind` variant breaks every exhaustive `match` on `NodeKind`; adding journal events breaks every exhaustive match on `JournalEvent`. That ripples across `orchestrator` and possibly `orchestrator-store` and `torii`. In SP-DATA-5 this was split across tasks and the result could not compile or commit at all, because the pre-commit hook lints the **whole workspace**. One commit must equal one compilable state.

So: enumerate the break sites with `cargo check --workspace --all-targets`, and fix them **mechanically** — new match arms that are inert, with a comment naming the task that gives them meaning. Do NOT implement behaviour here.

- [ ] **Step 1: Write the failing tests**

In `crates/orchestrator-core/src/journal.rs`'s test module:

```rust
    /// Additivity: this slice adds two NEW VARIANTS, not new fields. An old reader
    /// cannot know them, but a NEW reader must still load every OLD event unchanged —
    /// that is what keeps FORMAT_VERSION at 1.
    #[test]
    fn adding_the_signal_events_does_not_break_old_event_loading() {
        let old = r#"{"RunStarted":{"version":"v1"}}"#;
        let e: JournalEvent = serde_json::from_str(old).expect("old RunStarted still loads");
        assert!(matches!(e, JournalEvent::RunStarted { .. }));
    }

    #[test]
    fn the_signal_events_round_trip() {
        let awaited = JournalEvent::SignalAwaited {
            node: NodeId("gate".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::SignalAwaited { node, deadline } => {
                assert_eq!(node.0, "gate");
                assert!(deadline.is_some());
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let received = JournalEvent::SignalReceived {
            node: NodeId("gate".into()),
            payload: serde_json::json!({"decision": "approved"}),
        };
        let s = serde_json::to_string(&received).expect("serializes");
        let back: JournalEvent = serde_json::from_str(&s).expect("round-trips");
        match back {
            JournalEvent::SignalReceived { payload, .. } => {
                assert_eq!(payload["decision"], "approved")
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
```

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-orchestrator-core journal > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: non-zero — `no variant named SignalAwaited`.

- [ ] **Step 3: Add the events**

In `journal.rs`, beside `PlannerSelected`:

```rust
    /// SP-6 s1: an `AwaitSignal` node began waiting, recording its ABSOLUTE deadline.
    ///
    /// This exists as its own node-keyed event rather than relying on
    /// `RunPaused.resume_after` because that field is not node-keyed and a run pauses
    /// for many unrelated reasons over its life. Recording the absolute instant here is
    /// what stops the deadline being recomputed as `now + timeout` on every resume —
    /// which would push it forward forever, so a run force-woken every ten minutes with
    /// a one-hour timeout would NEVER expire.
    SignalAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// SP-6 s1: an external signal arrived for an `AwaitSignal` node. Folded by node id,
    /// so the node reads its answer and never re-asks — the same shape
    /// `PlannerSelected` uses for a planner choice. Last delivery wins while the node is
    /// still paused; once it has completed, the node is folded complete and never
    /// re-executes, so a later signal changes nothing.
    SignalReceived {
        node: NodeId,
        payload: serde_json::Value,
    },
```

- [ ] **Step 4: Add the node variant**

In `crates/orchestrator-core/src/graph.rs`'s `NodeKind`:

```rust
    /// SP-6 s1: pause until an external signal arrives for this node (HITL).
    ///
    /// `timeout` is a DURATION; the executor converts it to an absolute deadline ONCE,
    /// at first execution, and journals it (`SignalAwaited`). On the deadline with no
    /// signal the node FAILS — never a silent self-approval, which is why there is no
    /// default-payload option (spec §4).
    AwaitSignal {
        timeout: Option<chrono::Duration>,
    },
```

`chrono::Duration` may not implement `Serialize`. Check. If it does not, store the timeout as `timeout_secs: Option<i64>` and convert in the executor — report which you did, because Tasks 3 and 4 depend on the field name.

- [ ] **Step 5: Restore compilation, mechanically**

Run `cargo check --workspace --all-targets > /tmp/chk.log 2>&1; echo "exit=$?"` and fix every break:
- New `JournalEvent` match arms → inert, e.g. a label helper returns `"SignalAwaited"`.
- New `NodeKind` match arms → whatever the local match needs to stay exhaustive. **In `run_node` specifically**, add
  `NodeKind::AwaitSignal { .. } => unimplemented!("SP-6 s1 Task 3 implements this arm"),`
  so Task 3 has an obvious landing site and no test can silently exercise a half-built node.

Comment each inert arm with the task that fills it in.

- [ ] **Step 6: Verify and commit**

```bash
cargo check --workspace --all-targets; echo "exit=$?"
cargo test --workspace > /tmp/t.log 2>&1; echo "exit=$?"    # expect 0 at 1340 + 2
cargo fmt --all
git add -A
git commit -m "feat(core): SP-6 s1 (1/5) — SignalAwaited/SignalReceived events + AwaitSignal node"
```

---

## Task 2: fold the signals and the deadlines

**Files:** `crates/orchestrator/src/executor/mod.rs` (`Fold`), `crates/orchestrator/src/executor/support.rs` (`fold_journal`).

- [ ] **Step 1: Write the failing tests**

In `support.rs`'s test module, following the idiom of the neighbouring `fold_journal_*` tests:

```rust
    #[test]
    fn a_received_signal_is_folded_by_node_id() {
        let evs = vec![(
            0,
            JournalEvent::SignalReceived {
                node: NodeId("gate".into()),
                payload: serde_json::json!({"decision": "approved"}),
            },
        )];
        let (fold, _, _) = fold_journal(&evs);
        assert_eq!(fold.signal_for(&NodeId("gate".into())).unwrap()["decision"], "approved");
    }

    /// Last delivery wins while the node is still paused — an operator must be able to
    /// correct a mistaken decision before the run resumes.
    #[test]
    fn a_later_signal_overwrites_an_earlier_one_for_the_same_node() {
        let sig = |seq: Seq, d: &str| {
            (
                seq,
                JournalEvent::SignalReceived {
                    node: NodeId("gate".into()),
                    payload: serde_json::json!({ "decision": d }),
                },
            )
        };
        let (fold, _, _) = fold_journal(&[sig(0, "rejected"), sig(1, "approved")]);
        assert_eq!(fold.signal_for(&NodeId("gate".into())).unwrap()["decision"], "approved");
    }

    /// THE guard for this slice's trap. The deadline is recorded ONCE and folded
    /// thereafter; a second `SignalAwaited` must not move it. Recomputing `now + timeout`
    /// on each execution is the bug this pins.
    #[test]
    fn the_first_recorded_deadline_wins_and_is_never_moved() {
        let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_000_000, 0).unwrap();
        let t1 = chrono::DateTime::<chrono::Utc>::from_timestamp(9_000_000, 0).unwrap();
        let ev = |seq: Seq, d| {
            (
                seq,
                JournalEvent::SignalAwaited { node: NodeId("gate".into()), deadline: Some(d) },
            )
        };
        let (fold, _, _) = fold_journal(&[ev(0, t0), ev(1, t1)]);
        assert_eq!(
            fold.deadline_for(&NodeId("gate".into())),
            Some(t0),
            "the ORIGINAL deadline must survive; a later record must not extend it"
        );
    }

    #[test]
    fn a_node_with_no_signal_and_no_deadline_folds_to_none() {
        let (fold, _, _) = fold_journal(&[]);
        assert_eq!(fold.signal_for(&NodeId("gate".into())), None);
        assert_eq!(fold.deadline_for(&NodeId("gate".into())), None);
    }
```

Note the asymmetry the third test pins: **signals are last-wins, deadlines are FIRST-wins.** They are deliberately different and both are load-bearing. Say so in the code comments.

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-orchestrator support:: > /tmp/t2.log 2>&1; echo "exit=$?"`
Expected: non-zero — no method `signal_for` / `deadline_for`.

- [ ] **Step 3: Implement**

Add to `Fold` in `mod.rs`:

```rust
    /// SP-6 s1: signals received per node. LAST delivery wins — an operator correcting a
    /// mistaken decision before the run resumes is a supported workflow.
    signals: HashMap<NodeId, serde_json::Value>,
    /// SP-6 s1: the ABSOLUTE deadline each awaiting node recorded. FIRST record wins —
    /// the opposite of `signals`, and deliberately so: re-recording would let every
    /// resume push the deadline forward, and a run woken repeatedly would never expire.
    deadlines: HashMap<NodeId, chrono::DateTime<chrono::Utc>>,
```

and the accessors:

```rust
impl Fold {
    fn signal_for(&self, node: &NodeId) -> Option<&serde_json::Value> {
        self.signals.get(node)
    }

    fn deadline_for(&self, node: &NodeId) -> Option<chrono::DateTime<chrono::Utc>> {
        self.deadlines.get(node).copied()
    }
}
```

Adjust the test assertions if the borrow shapes differ (`Option<&Value>` vs `Option<Value>`) — keep the implementation borrow-friendly and fix the tests, not the other way round.

In `fold_journal`, add two **explicit** arms (never the `_` catch-all — SP-DATA-5's convention, because a silently-swallowed event here means a node that can never be signalled):

```rust
            JournalEvent::SignalReceived { node, payload } => {
                // Last wins: `insert` overwrites.
                fold.signals.insert(node.clone(), payload.clone());
            }
            JournalEvent::SignalAwaited { node, deadline } => {
                // FIRST wins: `entry().or_insert()`, NOT `insert`. Overwriting here is the
                // never-expires bug.
                if let Some(d) = deadline {
                    fold.deadlines.entry(node.clone()).or_insert(*d);
                }
            }
```

- [ ] **Step 4: Verify, then mutation-verify the trap**

Run: `cargo test -p sensei-orchestrator support::; echo "exit=$?"` — expect 0.

Then prove the deadline guard is real: change `entry().or_insert()` to `insert()`, confirm
`the_first_recorded_deadline_wins_and_is_never_moved` FAILS, restore. Report both outputs.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor
git commit -m "feat(orchestrator): SP-6 s1 (2/5) — fold signals (last wins) and deadlines (first wins)"
```

---

## Task 3: the node — a three-way fold read

**Files:** `crates/orchestrator/src/executor/mod.rs` (replace Task 1's `unimplemented!` arm; add `run_await_signal`), `crates/orchestrator/src/executor/tests.rs`.

- [ ] **Step 1: Write the failing tests**

Four tests, in `tests.rs`, following the idiom of the neighbouring `run_node` tests:

```
await_signal_completes_immediately_when_the_signal_is_already_folded   (AC3, early delivery)
await_signal_pauses_and_records_its_deadline_when_no_signal_is_present
await_signal_fails_when_the_deadline_has_passed_with_no_signal        (AC4)
await_signal_repauses_with_the_same_deadline_when_woken_early         (AC1 — the trap)
```

The fourth is the slice's most important test. Use `FakeClock` so time is controlled: seed a journal with a `SignalAwaited` whose deadline is one hour out, advance the fake clock by ten minutes, drive, assert the node re-pauses **and** that no second `SignalAwaited` with a different deadline was journaled. Repeat the wake three times and assert the deadline is still the original.

- [ ] **Step 2: Run and confirm they fail**

Run: `cargo test -p sensei-orchestrator await_signal > /tmp/t3.log 2>&1; echo "exit=$?"`
Expected: non-zero — the arm is `unimplemented!()` from Task 1, so these panic.

- [ ] **Step 3: Implement**

Replace Task 1's placeholder arm with `NodeKind::AwaitSignal { timeout } => self.run_await_signal(run, node, *timeout, fold).await,` and add:

```rust
    /// SP-6 s1: the HITL primitive — pause until a signal arrives, with an optional
    /// deadline that FAILS.
    ///
    /// A three-way fold read. The deadline is read from the fold and only computed (and
    /// journaled) on the FIRST execution: recomputing `now + timeout` on each drive would
    /// push it forward on every resume, so a run force-woken every ten minutes with a
    /// one-hour timeout would never expire.
    async fn run_await_signal(
        &self,
        run: RunId,
        node: &Node,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // 1. Answer already folded -> complete, never re-ask.
        if let Some(payload) = fold.signal_for(&node.id) {
            return Ok(NodeExec::Completed(payload.clone()));
        }

        // 2. Deadline: the folded one if this node has already waited, else compute and
        //    journal it ONCE.
        let deadline = match fold.deadline_for(&node.id) {
            Some(d) => Some(d),
            None => {
                let d = timeout.map(|t| self.clock.now() + t);
                self.journal
                    .append(run, JournalEvent::SignalAwaited { node: node.id.clone(), deadline: d })
                    .await
                    .map_err(OrchestratorError::Journal)?;
                d
            }
        };

        // 3. Deadline passed with no signal -> fail loudly. Never a silent self-approval.
        if let Some(d) = deadline
            && self.clock.now() >= d
        {
            return Ok(NodeExec::Failed {
                message: format!(
                    "await_signal: no signal for node {} by {d}",
                    node.id.0
                ),
                output: None,
            });
        }

        // 4. Still waiting. `resume_after` carries the ORIGINAL deadline, so the scheduler
        //    wakes it at the right absolute instant however many times it is woken early.
        Ok(NodeExec::Paused {
            reason: format!(
                "await_signal: waiting for a signal on node {}{}",
                node.id.0,
                deadline.map(|d| format!(" (deadline {d})")).unwrap_or_default()
            ),
        })
    }
```

**Note a gap you must close:** `NodeExec::Paused { reason }` carries no `resume_after`, so as written the pause would land in the journal's `RunPaused` with whatever `resume_after` the existing pause path supplies — probably `None`. Check how `NodeExec::Paused` becomes a `RunPaused` (around `mod.rs:781`) and thread the deadline through, or the timeout will never be auto-woken by the scheduler and the whole `Some(deadline)` branch is dead. Report exactly what you found and what you changed; this is the difference between a working timeout and a decorative one.

- [ ] **Step 4: Verify, then mutation-verify**

Run: `cargo test -p sensei-orchestrator await_signal; echo "exit=$?"` — expect 0.

Two mutations, each reverted after:
1. Compute the deadline unconditionally (`let deadline = timeout.map(|t| self.clock.now() + t);`, dropping the fold read) → `await_signal_repauses_with_the_same_deadline_when_woken_early` must FAIL.
2. Drop the `fold.signal_for` early return → `await_signal_completes_immediately_when_the_signal_is_already_folded` must FAIL.

Report both.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor
git commit -m "feat(orchestrator): SP-6 s1 (3/5) — the AwaitSignal node, with a journaled absolute deadline"
```

---

## Task 4: the torii surface

**Files:** `crates/torii/src/cmd/run.rs`, `crates/torii/src/main.rs`.

Start Postgres first (command at the top of this plan).

- [ ] **Step 1: Write the failing tests**

DB-free where possible, using `InMemoryJournal` + `InMemorySchedulerStore` as the existing `cmd/run.rs` tests do:

```
signal_appends_signal_received_and_reports_the_node          (happy path)
signal_on_a_completed_node_reports_not_delivered             (honest reporting, exit 2)
signal_on_a_run_that_is_not_paused_reports_not_delivered     (exit 2)
signal_on_an_unknown_run_exits_two
a_signal_payload_is_redacted_before_it_is_journaled          (AC6)
```

For AC6, assemble the credential at runtime (the repo's Semgrep CWE-798 hook blocks literal ones) and assert the journaled `SignalReceived.payload` contains no fragment of it.

- [ ] **Step 2: Run and confirm they fail**

- [ ] **Step 3: Implement**

`cmd::run::signal(store, journal, run, node, payload)`:

1. `store.status(run)` — unknown ⇒ `Outcome::precondition("no such run: …")`.
2. Not `Paused` ⇒ `Outcome::precondition` naming the actual state.
3. Redact the payload through `orchestrator_core::PatternRedactor` **before** appending — spec §6.4. Reuse whatever helper `render.rs` established for pause reasons rather than a second redaction path; if it is display-shaped and not reusable, add a small shared one and say so.
4. Append `SignalReceived { node, payload }`.
5. Report the effect, not the `Ok`: `signalled: <node> (the run will resume on the next worker tick)` — it must NOT claim the run resumed, exactly as `run wake` says "queued".

Then clap: `run signal <run-id> --node <node-id> --payload <json>`, light tier (it needs only the scheduler store and the journal, both already in `LightDeps`). Reject a payload that is not valid JSON with an actionable message.

Also extend `list-paused` to show the awaiting node and its deadline, so an operator can discover what to signal without reading the graph.

- [ ] **Step 4: Verify and commit**

```bash
cargo test -p sensei-torii; echo "exit=$?"
cargo fmt --all
git add crates/torii/src
git commit -m "feat(torii): SP-6 s1 (4/5) — torii run signal, reporting the effect achieved"
```

---

## Task 5: cross-process e2e, docs, final verification

**Files:** `crates/torii/tests/e2e_pg.rs`, `docs/superpowers/orchestrator-overview.md`.

- [ ] **Step 1: The e2e (AC7)**

Mirror the existing cross-process tests in that file — read them first, especially the **attributable-marker** technique (a bare `calls.len()` is flaky because `tick()` drives the whole due set of the shared `scheduled_runs` table). Take the `SCHEDULED_RUNS` guard.

Process A runs a graph to an `AwaitSignal` pause; assert `list_paused` surfaces the node; `torii run signal` delivers; a **fresh** worker (`serve --once`) completes the run; assert **zero re-spend** of the completed prefix via the attributable counter.

- [ ] **Step 2: Prove it discriminates**

Skip the signal delivery and confirm the test fails (the run stays paused rather than completing). Restore. Report both outputs.

- [ ] **Step 3: Docs**

Add an SP-6 s1 bullet to the overview's decision log in the established dense style, covering: HITL vs HOTL (SP-DATA-4 shipped human ON the loop; this is human IN it); the journaled-and-folded answer, and that `force_wake` is a resume not a decision; the journaled **absolute** deadline and the never-expires bug it prevents; signals last-wins vs deadlines first-wins and why they differ; the timeout failing rather than self-approving; payload redaction and that a signal is not a credential channel. Add the spec + plan to the index. Note SP-6 s2 (`HumanGate`) and s3 (human-as-Agent) as next.

- [ ] **Step 4: Final verification**

```bash
cargo fmt --all --check;                                             echo "fmt=$?"
cargo clippy --workspace --all-targets -- -D warnings;               echo "clippy=$?"
cargo test --workspace;                                              echo "ws-nodb=$?"
DATABASE_URL=postgres://postgres@localhost:5433/postgres cargo test --workspace; echo "ws-db=$?"
DATABASE_URL=... cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1; echo "orch=$?"
DATABASE_URL=... cargo test -p sensei-torii;                         echo "torii=$?"
```

All exit 0; workspace = **1340 + the new tests**, green both ways.

- [ ] **Step 5: Commit and push**

```bash
cargo fmt --all
git add -A
git commit -m "test(torii): SP-6 s1 (5/5) — cross-process AwaitSignal e2e; docs"
git push origin develop
```

---

## Self-Review

**Spec coverage.** §4's four decisions → Tasks 1 (events/variant), 2 (last-wins/first-wins), 3 (timeout fails), 4 (addressing by node id). §6.1 journaled deadline → Tasks 2 and 3, both mutation-checked. §6.2 three-way read → Task 3. §6.3 early signal → Task 3's first test. §6.4 redaction → Task 4 (AC6). §6.5 size cap → **see the gap below**. §6.6 honest reporting → Task 4. AC1→T3, AC2→T3, AC3→T3, AC4→T3, AC5→T4, AC6→T4, AC7→T5, AC8→T5.

**One spec requirement with no task, named rather than dropped:** §6.5's payload size cap. It is not in Tasks 1-5 because the right mechanism depends on how `split_output`'s `cas_threshold` composes with a journal event that is not an `EffectRecorded` — and that is a design question, not mechanical work. **Task 4 must report what it found**, and if routing through the CAS is not straightforward, the honest interim is a CLI-side length limit with the durable-side gap recorded as a carry-forward. Do not let it pass silently.

**Placeholders:** none. Tasks 4 and 5 describe test bodies rather than spelling them out, because they depend on harness shapes (`LightDeps`'s journal, the e2e's marker technique) the implementer must read first; each names exactly what to read and what to assert.

**Type consistency:** `SignalAwaited { node, deadline }` / `SignalReceived { node, payload }` from Task 1 are used unchanged in Tasks 2-4. `Fold::signal_for` / `deadline_for` from Task 2 are the exact names Task 3 calls. `NodeKind::AwaitSignal { timeout }` from Task 1 is matched in Task 3 — **subject to Task 1's report on whether `chrono::Duration` serializes**; if it becomes `timeout_secs: Option<i64>`, Task 3's signature changes with it.

**Two risks named:**
1. Task 3's `NodeExec::Paused` carries no `resume_after`. If threading the deadline through proves invasive, the timeout branch is decorative and the slice's headline feature does not work — the same class as SP-DATA-5's frozen ledger. Task 3 must report what it found here explicitly.
2. Task 1's ripple may be wider than SP-DATA-5's (a `NodeKind` variant is matched in more places than a `JournalEvent` one). If the mechanical fixes exceed what one commit can sensibly hold, say so rather than splitting into a non-compiling state.
