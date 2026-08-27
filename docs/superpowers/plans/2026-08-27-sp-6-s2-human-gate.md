# SP-6 s2 — `HumanGate` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeKind::HumanGate` — a node that asks a human to pick one of an enumerated set of options, where each option declares whether the run continues or terminates.

**Architecture:** A typed layer over SP-6 s1's `AwaitSignal`. Two new journal events (`GateAwaited` carries the durable menu, `GateDecided` the typed answer) — both new *variants*, so `FORMAT_VERSION` stays 1. s1's `run_await_signal` is **split** into three pieces so the fail-closed terminal guard and the deadline durability are shared rather than copied; s1's whole-slice review found real defects in exactly those two arms. Validation bites in two layers: `torii` refuses before writing, the executor is authoritative.

**Tech Stack:** Rust 2024, `tokio`, `chrono`, `serde`/`serde_json`, `clap` 4 derive, `sqlx` (Postgres, feature-gated), `async_trait`.

**Spec:** `docs/superpowers/specs/2026-08-27-sp-6-s2-human-gate-design.md`

**Baseline that must not regress:** `env -u DATABASE_URL cargo test --workspace` = **1427 passed / 0 failed / 7 ignored**, exit 0.

---

## Ground rules for every task

- **TDD, strictly.** Write the failing test, *run it and watch it fail*, then implement. A test that never went red proves nothing.
- **Verify real exit codes.** Never `cargo test … | tail` — the pipe's status is not the command's. Run the command, read `$?`. In zsh, `PIPESTATUS` is `pipestatus`.
- **Commit messages via stdin** (`git commit -F -` with a heredoc), never `-m "…"`. Backticks in `-m` are command-substituted by zsh and silently delete identifiers from the message.
- **`cargo fmt --all` before every commit.** The pre-commit hook runs `fmt --check` + `clippy -D warnings` but runs **no tests** — run `cargo test --workspace` yourself.
- Secrets in fixtures must be **assembled at runtime** (`format!("sk-{}", "A".repeat(24))`). The repo's Semgrep CWE-798 hook blocks credential-shaped literals.

## File structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/graph.rs` | `NodeKind::HumanGate`, `GateOption`, `GateOutcome`, `validate_dag` rules | Modify |
| `crates/orchestrator-core/src/journal.rs` | `GateAwaited` / `GateDecided` variants | Modify |
| `crates/orchestrator/src/executor/support.rs` | `fold_journal` arms for the two new events | Modify |
| `crates/orchestrator/src/executor/mod.rs` | `Fold.gate_decisions` + accessor; node dispatch arm | Modify |
| `crates/orchestrator/src/executor/signal.rs` | Split into `gate_precheck` / `wait_or_expire`; keep `run_await_signal` | Modify |
| `crates/orchestrator/src/executor/gate.rs` | **`run_human_gate`** — the new node | **Create** |
| `crates/torii/src/cmd/gate.rs` | `gate approve/reject/decide` command logic | **Create** |
| `crates/torii/src/cmd/run.rs` | `signal` refuses a `HumanGate`; `list-paused` shows menus | Modify |
| `crates/torii/src/main.rs` | `GateAction` clap subcommand + dispatch | Modify |
| `crates/torii/src/render.rs` | Awaiting-row rendering for gates | Modify |

`gate.rs` is a new file rather than more of `signal.rs` because `executor/` is already a directory module split by concern, and `signal.rs` after the split holds the *shared* wait machinery — mixing a second node kind into it would undo the separation Task 3 creates.

---

## Task 1: The two journal events

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs` (add variants after `SignalReceived`, ~line 205)
- Test: same file, `mod tests`

- [ ] **Step 1: Write the failing test**

Add to `crates/orchestrator-core/src/journal.rs`, inside `mod tests`:

```rust
    /// SP-6 s2: both new variants round-trip, and — the load-bearing half — they are
    /// new VARIANTS, so an event written by an older binary still loads. That is what
    /// keeps `FORMAT_VERSION` at 1.
    #[test]
    fn the_gate_events_round_trip_without_a_format_bump() {
        use crate::graph::{GateOption, GateOutcome};

        let awaited = JournalEvent::GateAwaited {
            node: NodeId("release".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
            options: vec![
                GateOption { name: "ship".into(), outcome: GateOutcome::Complete },
                GateOption { name: "hold".into(), outcome: GateOutcome::Fail },
            ],
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::GateAwaited {
                node,
                deadline,
                options,
            } => {
                assert_eq!(node.0, "release");
                assert!(deadline.is_some());
                assert_eq!(options.len(), 2);
                assert_eq!(options[0].name, "ship");
                // Both outcomes are durable AND DISTINCT. A menu whose options are all
                // `Complete` passes even if the two variants collapse into one on the
                // wire — which would make every rejected gate resume as an approval.
                assert_eq!(options[0].outcome, GateOutcome::Complete);
                assert_eq!(options[1].outcome, GateOutcome::Fail);
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let decided = JournalEvent::GateDecided {
            node: NodeId("release".into()),
            option: "ship".into(),
            actor: "alice".into(),
            note: Some("capped at 5k".into()),
        };
        let s = serde_json::to_string(&decided).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::GateDecided {
                node,
                option,
                actor,
                note,
            } => {
                assert_eq!(node.0, "release");
                assert_eq!(option, "ship");
                assert_eq!(actor, "alice");
                assert_eq!(note.as_deref(), Some("capped at 5k"));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        // A `note`-less decision is legal: a Complete option needs no reason.
        let terse = JournalEvent::GateDecided {
            node: NodeId("release".into()),
            option: "ship".into(),
            actor: "ci".into(),
            note: None,
        };
        let s = serde_json::to_string(&terse).expect("serializes");
        assert!(serde_json::from_str::<JournalEvent>(&s).is_ok());

    }
```

- [ ] **Step 2: Run it and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib the_gate_events_round_trip
```

Expected: **compile error** — `no variant named GateAwaited found for enum JournalEvent`.

- [ ] **Step 3: Add the two data types the events carry**

`GateAwaited` carries `Vec<GateOption>`, so the type must exist before the event does.
These are pure data with no behaviour — the `NodeKind` variant that USES them, and its
validation, are Task 2.

In `crates/orchestrator-core/src/graph.rs`, after the `MAX_AWAIT_SIGNAL_TIMEOUT` const:

```rust
/// One choice a [`NodeKind::HumanGate`] offers, and what picking it does to the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateOption {
    /// What the operator types: `torii run gate decide … --option <name>`.
    pub name: String,
    pub outcome: GateOutcome,
}

/// What choosing a [`GateOption`] does to the run.
///
/// Per-option rather than a fixed approve/reject pair, so a three-way gate
/// (`ship | hold | escalate`) needs no special case — and deliberately reusing the
/// EXISTING terminal machinery, so this slice needs no new `RunStatus`, no
/// `SchedulerStore` change and no dbd migration.
///
/// **Accepted cost:** a `Fail` option and a dead provider both surface as
/// `RunStatus::Failed`, so they are indistinguishable BY STATUS — only the reason text
/// tells them apart, and only `torii run status` renders it. Anything filtering on status
/// conflates them: a script, or the terminal allowlist in
/// `count_terminal_before`/`prune_terminal`. (NOT `list-paused`: it filters
/// `status='paused'` and both cases are terminal, so neither appears there at all.)
/// A distinct `Rejected` status would be more truthful but reaches both store impls, the
/// dbd CHECK constraint and torii's rendering — deferred, not overlooked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateOutcome {
    /// The decision becomes this node's output; dependents run.
    Complete,
    /// `NodeFailed`; hard-edge dependents cascade-skip.
    Fail,
}
```

Re-export them from the crate root alongside the other graph types so
`orchestrator_core::GateOption` resolves (match however `NodeKind`/`BranchCond` are
re-exported in `crates/orchestrator-core/src/lib.rs`).

- [ ] **Step 4: Add the variants**

In `crates/orchestrator-core/src/journal.rs`, immediately after the `SignalReceived` variant:

```rust
    /// SP-6 s2: a `HumanGate` has begun asking, carrying the MENU the human was shown.
    ///
    /// The options are journaled rather than re-read from the graph for the same reason
    /// s1 journals the deadline: a human was shown a menu, and validating their answer
    /// against a *different* menu later is simply wrong. Nothing BINDS the graph a later
    /// drive is handed to the one the human was shown — `Executor::start` takes it as a
    /// caller parameter, no fence covers it (the config-version fence covers the registry),
    /// and the executor cannot see `SchedulerStore`. So an author who edits the graph
    /// between drives silently rewrites the menu. `scheduled_runs.graph` happens to hold a
    /// copy on the scheduler path, but the executor cannot read it.
    ///
    /// The full [`GateOption`]s, not just their names: the OUTCOME the human was shown
    /// ("reject will stop the run") is as much a part of the offer as the name. If only
    /// names were journaled, an author flipping `reject` from `Fail` to `Complete` after
    /// a human rejected would silently change what their recorded answer MEANT.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited` does — overwriting the
    /// deadline is the never-expires bug.
    GateAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: Vec<GateOption>,   // `use crate::graph::{GateOption, Graph};` at the top
    },
    /// SP-6 s2: a human picked one of a `HumanGate`'s options.
    ///
    /// A `HumanGate` is answerable ONLY by this event, never by `SignalReceived` — if a
    /// raw signal could answer one, `torii run signal --payload '{}'` would bypass every
    /// validation the slice adds.
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION: it is whatever string the caller
    /// supplied, so this answers "who claimed to decide", not "who decided". `note` is
    /// `Option` because a `Complete` decision legitimately has none; the CLI separately
    /// requires one for a `Fail` option (a documentation rule, not a safety rule).
    GateDecided {
        node: NodeId,
        option: String,
        actor: String,
        note: Option<String>,
    },
```

- [ ] **Step 5: Run it and watch it pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib the_gate_events_round_trip
```

Expected: `test result: ok. 1 passed`.

Then the whole crate, to catch any non-exhaustive `match` this broke:

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core
```

If other crates fail to compile on a non-exhaustive match, add the arms in Task 2/3 — do not add a catch-all `_ =>`, which would silently swallow future variants.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/lib.rs
git commit -F - <<'MSGEOF'
feat(core): SP-6 s2 (1/8) — GateOption/GateOutcome + the two gate events

Two new variants, not new fields, so FORMAT_VERSION stays 1 and an event written
by an older binary still loads — the same additivity trick s1 used.

GateAwaited carries the MENU. The options are journaled rather than re-read from
the graph because a human was shown a menu, and validating their answer against a
different menu later is simply wrong; the graph is caller-supplied and never
journaled (SP-DATA-3), so nothing else makes it durable. Same argument s1 made for
the deadline.

GateDecided.actor is ATTRIBUTION, NOT AUTHENTICATION, and the doc comment says so:
anyone who can reach the database can write any actor string.
MSGEOF
```

---

## Task 2: The node kind and its `validate_dag` rules

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs` — `NodeKind` (after `AwaitSignal`, ~line 88), new types, `validate_dag` block `2b-ter`
- Test: same file, `mod tests`

- [ ] **Step 1: Write the failing tests**

Add to `crates/orchestrator-core/src/graph.rs`, inside `mod tests`:

```rust
    fn gate(options: Vec<GateOption>, timeout: Option<chrono::Duration>) -> Graph {
        Graph {
            nodes: vec![Node {
                id: NodeId("release".into()),
                kind: NodeKind::HumanGate { options, timeout },
                deps: vec![],
            }],
        }
    }

    fn opt(name: &str, outcome: GateOutcome) -> GateOption {
        GateOption {
            name: name.to_string(),
            outcome,
        }
    }

    /// A gate must offer a real choice, and at least one way FORWARD. Same principle as
    /// `max_iters == 0` and a non-positive timeout: reject the degenerate node loudly at
    /// validation rather than let it produce a baffling runtime state.
    #[test]
    fn a_degenerate_gate_is_rejected() {
        // No options at all: nothing to pick.
        let e = gate(vec![], None).validate_dag().expect_err("empty options");
        assert!(format!("{e}").contains("at least one option"), "{e}");

        // Every option fails: the run can NEVER proceed past this node, so the graph
        // is a guaranteed dead end however the human answers.
        let e = gate(
            vec![opt("reject", GateOutcome::Fail), opt("deny", GateOutcome::Fail)],
            None,
        )
        .validate_dag()
        .expect_err("no Complete option");
        assert!(format!("{e}").contains("at least one Complete"), "{e}");

        // Duplicate names: `decide --option approve` would be ambiguous.
        let e = gate(
            vec![
                opt("approve", GateOutcome::Complete),
                opt("approve", GateOutcome::Fail),
            ],
            None,
        )
        .validate_dag()
        .expect_err("duplicate names");
        assert!(format!("{e}").contains("duplicate"), "{e}");

        // An empty name cannot be typed at the CLI.
        let e = gate(vec![opt("", GateOutcome::Complete)], None)
            .validate_dag()
            .expect_err("empty name");
        assert!(format!("{e}").contains("empty"), "{e}");
    }

    /// The timeout bounds are s1's, reused verbatim — a `HumanGate` computes `now +
    /// timeout` through the same shared code path, so it can overflow `DateTime<Utc>`
    /// the same way and poison a worker the same way.
    #[test]
    fn a_gate_timeout_obeys_the_same_bounds_as_await_signal() {
        let ok = vec![opt("approve", GateOutcome::Complete)];

        let e = gate(ok.clone(), Some(chrono::Duration::zero()))
            .validate_dag()
            .expect_err("zero timeout");
        assert!(format!("{e}").contains("non-positive"), "{e}");

        let e = gate(ok.clone(), Some(chrono::Duration::hours(-1)))
            .validate_dag()
            .expect_err("negative timeout");
        assert!(format!("{e}").contains("non-positive"), "{e}");

        let e = gate(
            ok.clone(),
            Some(MAX_AWAIT_SIGNAL_TIMEOUT + chrono::Duration::days(1)),
        )
        .validate_dag()
        .expect_err("over the century bound");
        assert!(format!("{e}").contains("too long"), "{e}");

        // The legitimate range still validates.
        gate(ok.clone(), None).validate_dag().expect("indefinite");
        gate(ok.clone(), Some(chrono::Duration::hours(48)))
            .validate_dag()
            .expect("48h SLA");
        gate(ok, Some(MAX_AWAIT_SIGNAL_TIMEOUT))
            .validate_dag()
            .expect("exactly the bound");
    }
```

- [ ] **Step 2: Run and watch them fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_degenerate_gate a_gate_timeout_obeys
```

Expected: **compile error** — `cannot find type GateOption`.

- [ ] **Step 3: Add the types and the validation**

In `crates/orchestrator-core/src/graph.rs`, add to `NodeKind` after `AwaitSignal`:

```rust
    /// SP-6 s2: ask a human to pick one of an enumerated set of options, each of which
    /// declares its own outcome. The typed layer over `AwaitSignal`.
    HumanGate {
        options: Vec<GateOption>,
        timeout: Option<chrono::Duration>,
    },
```

`GateOption` and `GateOutcome` already exist — Task 1 added them, because `GateAwaited`
carries them. This task adds only the node kind that USES them, and its validation.

In `validate_dag`, immediately after the existing `2b-bis` block:

```rust
        // 2b-ter. SP-6 s2: a `HumanGate`'s menu must be usable, and must offer a way
        // FORWARD. Same principle as `max_iters == 0` and the non-positive timeout
        // above: reject the degenerate node loudly here rather than let it produce a
        // baffling runtime state. A gate whose every option Fails is a guaranteed dead
        // end however the human answers — which is a malformed graph, not a policy.
        //
        // The timeout bounds are s1's, applied to this kind too: a `HumanGate` computes
        // `now + timeout` through the SAME shared `wait_or_expire`, so an unbounded one
        // overflows `DateTime<Utc>` and poisons a worker identically.
        for node in &self.nodes {
            let NodeKind::HumanGate { options, timeout } = &node.kind else {
                continue;
            };
            if options.is_empty() {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "human_gate node {:?} declares no options; it must offer at least one option",
                    node.id
                )));
            }
            if !options.iter().any(|o| o.outcome == GateOutcome::Complete) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "human_gate node {:?} has no Complete option, so the run can never \
                     proceed past it however the human answers; at least one Complete \
                     option is required",
                    node.id
                )));
            }
            let mut seen = std::collections::HashSet::new();
            for o in options {
                if o.name.is_empty() {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "human_gate node {:?} has an option with an empty name; an \
                         operator could not type it",
                        node.id
                    )));
                }
                if !seen.insert(o.name.as_str()) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "human_gate node {:?} has a duplicate option name {:?}; \
                         `--option {}` would be ambiguous",
                        node.id, o.name, o.name
                    )));
                }
            }
            if let Some(t) = timeout {
                if *t <= chrono::Duration::zero() {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "human_gate node {:?} has a non-positive timeout ({t}); \
                         use `None` to wait indefinitely",
                        node.id
                    )));
                }
                if *t > MAX_AWAIT_SIGNAL_TIMEOUT {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "human_gate node {:?} has a timeout ({t}) that is too long; \
                         the maximum is {MAX_AWAIT_SIGNAL_TIMEOUT}",
                        node.id
                    )));
                }
            }
        }
```

- [ ] **Step 4: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_degenerate_gate a_gate_timeout_obeys
```

Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: Fix the doc guard this WILL break**

`every_node_kind_is_named_in_the_execution_graph_feature_doc` (added in the s1 review) scrapes `NodeKind` and asserts each variant appears in the feature doc. It now fails on `HumanGate` — by design.

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib every_node_kind_is_named
```

Expected: **FAIL** with `node kinds implemented but absent from docs/…: ["HumanGate"]`.

Add to `docs/features/orchestrator/execution-graph.md`, in the `> - **\`Expand …\`**` bullet list:

```markdown
> - **`HumanGate { options, timeout }`** (SP-6 s2) — the TYPED layer over `AwaitSignal`:
>   a human picks one of an enumerated menu, and each `GateOption` declares its own
>   `GateOutcome` — `Complete` (the decision becomes the node's output, dependents run)
>   or `Fail` (`NodeFailed`, hard-edge dependents cascade-skip). Output on `Complete` is
>   `{"decision","actor","note"}`, which `BranchCond::FieldEquals("decision", …)` matches
>   directly, so `Branch` is reused unchanged. The MENU IS DURABLE: `GateAwaited` journals
>   the options the human was actually shown, so editing the graph cannot retroactively
>   change what their answer meant. Answerable ONLY by `GateDecided` — a raw
>   `SignalReceived` on a gate is ignored. `validate_dag` rejects an empty menu, duplicate
>   or empty option names, a menu with no `Complete` option (a guaranteed dead end), and
>   the same timeout bounds as `AwaitSignal`. Operator surface:
>   `torii run gate approve|reject|decide`.
```

Re-run:

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib every_node_kind_is_named
```

Expected: `ok. 1 passed`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs docs/features/orchestrator/execution-graph.md
git commit -F - <<'MSGEOF'
feat(core): SP-6 s2 (2/7) — NodeKind::HumanGate + its validate_dag rules

GateOption{name, outcome} with GateOutcome::{Complete, Fail}. Per-option rather
than a fixed approve/reject pair, so ship|hold|escalate needs no special case,
and reusing the EXISTING terminal machinery so s2 needs no new RunStatus, no
SchedulerStore change and no dbd migration.

validate_dag block 2b-ter rejects the degenerate node loudly, following the
max_iters == 0 precedent: an empty menu, an empty or duplicate option name, and
a menu with NO Complete option — which is a guaranteed dead end however the human
answers, i.e. a malformed graph rather than a policy.

The timeout bounds are s1's, applied to this kind too, because a HumanGate
computes `now + timeout` through the SAME shared wait path and so overflows
DateTime<Utc> and poisons a worker identically.

The s1-review doc guard (every_node_kind_is_named_in_the_execution_graph_feature_doc)
failed on HumanGate exactly as designed, and the feature doc is updated. That
guard scrapes the enum, so the next node kind cannot ship undocumented either.
MSGEOF
```

---

## Task 3: Split s1's node into shared pieces

This task is a **pure refactor — no behaviour change**. The existing s1 tests are the guard: they must stay green throughout without modification.

**Files:**
- Modify: `crates/orchestrator/src/executor/signal.rs`

- [ ] **Step 1: Confirm the s1 suite is green before touching anything**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib await_signal
```

Expected: all pass. Record the count — it must not change.

- [ ] **Step 2: Extract the two shared pieces**

In `crates/orchestrator/src/executor/signal.rs`, add above `run_await_signal`:

```rust
/// What a waiting node's shared machinery decided, when no answer is present.
pub(super) enum WaitState {
    /// The node already failed and stays failed — the fail-closed arm.
    AlreadyFailed(String),
    /// Nothing is recorded for this node yet; the caller must journal its own
    /// "now asking" event, then re-enter. Carries the deadline to record.
    NotYetAsking(Option<chrono::DateTime<chrono::Utc>>),
    /// The node is asking and the deadline has passed with no answer.
    Expired(chrono::DateTime<chrono::Utc>),
    /// The node is asking and still has time.
    Waiting(Option<chrono::DateTime<chrono::Utc>>),
}

impl Executor {
    /// Arm 0 of §6.2, shared by BOTH waiting node kinds: a folded `NodeFailed` for this
    /// node is TERMINAL, read back rather than re-derived, and checked BEFORE any answer.
    ///
    /// Shared rather than copied deliberately. s1's whole-slice review found that without
    /// this check a late `SignalReceived` drove a run that had terminally failed on its
    /// deadline all the way to `RunCompleted` carrying `{"decision":"approved"}` — the
    /// silent self-approval §4 rejects, reached by the back door. A second copy of this
    /// logic is a second place for that to come back.
    ///
    /// This is the ONLY consumer family of `fold.failed`: a `NodeFailed` does NOT make a
    /// node terminal in general (a `ModelCall`/`Agent` whose provider died re-attempts on
    /// resume, by design and by test). A waiting node is the one kind whose failure is
    /// irreversible by construction, because the thing that failed is an instant that has
    /// passed.
    pub(super) fn gate_precheck(&self, node: &Node, fold: &Fold) -> Option<NodeExec> {
        fold.failure_for(&node.id).map(|error| NodeExec::Failed {
            message: error.to_string(),
            output: None,
        })
    }

    /// Arms 2–4 of §6.2, shared: read the recorded deadline or compute a fresh one, and
    /// report whether the node has begun asking, has expired, or is still waiting.
    ///
    /// **The deadline is READ from the fold, never recomputed.** The obvious `now +
    /// timeout` on every execution is wrong in a way a naive test does not catch: every
    /// resume pushes it forward, so a run force-woken every ten minutes with a one-hour
    /// timeout would NEVER expire.
    ///
    /// `checked_add_signed`, not `+`: `chrono::Duration` reaches ~292 million years while
    /// `DateTime<Utc>` stops at year 262143, so the plain `+` PANICS on a large timeout —
    /// and that panic unwinds through `Scheduler::tick` (which has already claimed a batch
    /// and taken their leases) and out of `worker::serve`, killing the worker and leaving
    /// a row the next worker reclaims and dies on identically. `validate_dag` rejects such
    /// a timeout, but `Executor::start` takes the graph as a caller parameter and nothing
    /// guarantees it was validated, so a node kind may not panic on its own.
    pub(super) fn wait_or_expire(
        &self,
        node: &Node,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<WaitState, String> {
        if let Some(error) = fold.failure_for(&node.id) {
            return Ok(WaitState::AlreadyFailed(error.to_string()));
        }
        let Some(recorded) = fold.deadline_for(&node.id) else {
            let fresh = match timeout {
                None => None,
                Some(t) => match self.clock.now().checked_add_signed(t) {
                    Some(instant) => Some(instant),
                    None => {
                        return Err(format!(
                            "node {} has a timeout ({t}) that overflows the representable \
                             instant range when added to now",
                            node.id.0
                        ));
                    }
                },
            };
            return Ok(WaitState::NotYetAsking(fresh));
        };
        match recorded {
            Some(d) if self.clock.now() >= d => Ok(WaitState::Expired(d)),
            other => Ok(WaitState::Waiting(other)),
        }
    }

    /// The durable pause both waiting kinds end on. `resume_after` carries the ORIGINAL
    /// absolute deadline, so the scheduler re-arms on the same instant however many times
    /// the run is woken early — without which the whole timed branch would be decorative.
    /// `None` is SP-DATA-3's never-auto-woken class: the indefinite human gate.
    pub(super) async fn pause_awaiting(
        &self,
        run: RunId,
        reason: String,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(
            run,
            JournalEvent::RunPaused {
                reason: reason.clone(),
                resume_after: deadline,
            },
        )
        .await?;
        Ok(NodeExec::Paused { reason })
    }
}
```

- [ ] **Step 3: Rewrite `run_await_signal` to use them**

Replace the body of `run_await_signal` (keep the whole existing doc comment — it documents behaviour that has not changed):

```rust
    pub(super) async fn run_await_signal(
        &self,
        run: RunId,
        node: &Node,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        if let Some(failed) = self.gate_precheck(node, fold) {
            return Ok(failed);
        }
        if let Some(payload) = fold.signal_for(&node.id) {
            return Ok(NodeExec::Completed(self.redact(payload)));
        }
        let deadline = match self.wait_or_expire(node, timeout, fold) {
            Err(message) => {
                let message = format!("await_signal: {message}");
                self.append(
                    run,
                    JournalEvent::NodeFailed {
                        node: node.id.clone(),
                        error: message.clone(),
                    },
                )
                .await?;
                return Ok(NodeExec::Failed {
                    message,
                    output: None,
                });
            }
            Ok(WaitState::AlreadyFailed(message)) => {
                return Ok(NodeExec::Failed {
                    message,
                    output: None,
                });
            }
            Ok(WaitState::NotYetAsking(fresh)) => {
                self.append(
                    run,
                    JournalEvent::SignalAwaited {
                        node: node.id.clone(),
                        deadline: fresh,
                    },
                )
                .await?;
                fresh
            }
            Ok(WaitState::Expired(d)) => {
                let message = format!("await_signal: no signal for node {} by {d}", node.id.0);
                self.append(
                    run,
                    JournalEvent::NodeFailed {
                        node: node.id.clone(),
                        error: message.clone(),
                    },
                )
                .await?;
                return Ok(NodeExec::Failed {
                    message,
                    output: None,
                });
            }
            Ok(WaitState::Waiting(d)) => d,
        };

        // A freshly recorded deadline can ALREADY have passed: step 2 fixes it from one
        // `now` and this reads the clock again after an awaited append, so `timeout:
        // Some(1ns)` journals `SignalAwaited` then `NodeFailed` in a single execution
        // (measured). Correct and loud — a gate given a nanosecond has genuinely expired.
        if let Some(d) = deadline
            && self.clock.now() >= d
        {
            let message = format!("await_signal: no signal for node {} by {d}", node.id.0);
            self.append(
                run,
                JournalEvent::NodeFailed {
                    node: node.id.clone(),
                    error: message.clone(),
                },
            )
            .await?;
            return Ok(NodeExec::Failed {
                message,
                output: None,
            });
        }

        let reason = format!(
            "await_signal: waiting for a signal on node {}{}",
            node.id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }
```

- [ ] **Step 4: Prove the refactor changed nothing**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib await_signal
env -u DATABASE_URL cargo test --workspace
```

Expected: the same count as Step 1, `0 failed`, exit 0. **No s1 test may be edited.** If one fails, the refactor changed behaviour — fix the refactor, not the test.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/signal.rs
git commit -F - <<'MSGEOF'
refactor(orchestrator): SP-6 s2 (3/7) — split the waiting node into shared pieces

Pure refactor, no behaviour change: every s1 test passes unmodified.

s1's run_await_signal is six arms and the whole-slice review found real defects
in two of them — the fail-closed terminal guard (a late signal drove a
deadline-failed run to RunCompleted carrying "approved") and the deadline
durability (recomputing `now + timeout` per execution means a run force-woken
every ten minutes with a one-hour timeout NEVER expires).

s2 needs both. Copying them would create a second place for both defects to come
back, so they are extracted instead: gate_precheck (arm 0), wait_or_expire (arms
2-4, including the checked_add_signed overflow guard) and pause_awaiting. The
per-kind part — what counts as an answer — stays in each node.
MSGEOF
```

---

## Task 4: Fold the gate events

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` — `Fold` struct + accessor
- Modify: `crates/orchestrator/src/executor/support.rs` — `fold_journal` arms
- Test: `crates/orchestrator/src/executor/support.rs`, `mod tests`

- [ ] **Step 1: Write the failing test**

Add to `crates/orchestrator/src/executor/support.rs`, inside `mod tests`:

```rust
    /// The two fold asymmetries are OPPOSITE and both load-bearing, exactly as s1's are.
    #[test]
    fn gate_decisions_are_last_wins_and_the_menu_is_first_wins() {
        let events = vec![
            (
                1,
                JournalEvent::GateAwaited {
                    node: NodeId("release".into()),
                    deadline: Some(at(1_000)),
                    options: vec![gopt("ship", GateOutcome::Complete), gopt("hold", GateOutcome::Complete)],
                },
            ),
            (
                2,
                JournalEvent::GateDecided {
                    node: NodeId("release".into()),
                    option: "hold".into(),
                    actor: "alice".into(),
                    note: None,
                },
            ),
            // An operator corrects themselves before the run resumes: LAST wins.
            (
                3,
                JournalEvent::GateDecided {
                    node: NodeId("release".into()),
                    option: "ship".into(),
                    actor: "alice".into(),
                    note: Some("legal cleared it".into()),
                },
            ),
            // A second ask must NOT move the deadline or the menu: FIRST wins.
            // Overwriting the deadline IS the never-expires bug.
            (
                4,
                JournalEvent::GateAwaited {
                    node: NodeId("release".into()),
                    deadline: Some(at(9_999)),
                    options: vec![gopt("escalate", GateOutcome::Complete)],
                },
            ),
        ];
        let fold = fold_journal(&events);

        let d = fold
            .gate_decision_for(&NodeId("release".into()))
            .expect("decided");
        assert_eq!(d.option, "ship", "LAST decision wins");
        assert_eq!(d.actor, "alice");
        assert_eq!(d.note.as_deref(), Some("legal cleared it"));

        assert_eq!(
            fold.deadline_for(&NodeId("release".into())),
            Some(Some(at(1_000))),
            "FIRST ask wins — a later one must not push the deadline forward"
        );
        assert_eq!(
            fold.menu_for(&NodeId("release".into())).map(|m| m.len()),
            Some(2),
            "FIRST menu wins — the human was shown THIS menu, not the later one-option ask"
        );
        assert_eq!(
            fold.menu_for(&NodeId("release".into())).unwrap()[0].name,
            "ship"
        );
    }

    /// The indefinite gate: `None` is folded as a REAL value, so the node's "have I begun
    /// asking?" question is answered by the KEY, not by the value. Without this the node
    /// re-journals `GateAwaited` on every drive.
    #[test]
    fn a_deadline_less_gate_records_that_it_began_asking() {
        let events = vec![(
            1,
            JournalEvent::GateAwaited {
                node: NodeId("release".into()),
                deadline: None,
                options: vec![gopt("approve", GateOutcome::Complete)],
            },
        )];
        let fold = fold_journal(&events);
        assert_eq!(fold.deadline_for(&NodeId("release".into())), Some(None));
        assert!(fold.menu_for(&NodeId("release".into())).is_some());
    }
```

If `at()` does not already exist in that test module, add:

```rust
    fn at(unix_secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::<chrono::Utc>::from_timestamp(unix_secs, 0).expect("valid timestamp")
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib gate_decisions_are_last_wins a_deadline_less_gate_records
```

Expected: **compile error** — `no method named gate_decision_for`.

- [ ] **Step 3: Add the fold state**

In `crates/orchestrator/src/executor/mod.rs`, add to `struct Fold` after `deadlines`:

```rust
    /// SP-6 s2: each `HumanGate`'s decision, folded from `GateDecided`. LAST wins, like
    /// `signals` and for the same reason: an operator must be able to correct a mistaken
    /// decision before the run resumes.
    gate_decisions: HashMap<NodeId, GateDecision>,
    /// SP-6 s2: the MENU each `HumanGate` published when it began asking, folded from
    /// `GateAwaited`. FIRST wins — the human was shown THIS menu, and a later ask must
    /// not retroactively change what their answer meant.
    ///
    /// `deadlines` is folded from `GateAwaited` too, so the "has this node begun asking?"
    /// question stays in one place for both waiting kinds.
    menus: HashMap<NodeId, Vec<orchestrator_core::GateOption>>,
```

Add the value type near `Fold`:

```rust
/// SP-6 s2: a folded `GateDecided`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct GateDecision {
    pub option: String,
    /// ATTRIBUTION, NOT AUTHENTICATION — see `JournalEvent::GateDecided`.
    pub actor: String,
    pub note: Option<String>,
}
```

Add the accessors in `impl Fold`, next to `failure_for`:

```rust
    /// SP-6 s2: the decision folded for this `HumanGate`, if a human has answered.
    fn gate_decision_for(&self, node: &NodeId) -> Option<&GateDecision> {
        self.gate_decisions.get(node)
    }

    /// SP-6 s2: the menu this gate published when it began asking. `None` = it has not
    /// asked yet, which is what makes a decision-without-a-menu detectable.
    fn menu_for(&self, node: &NodeId) -> Option<&[orchestrator_core::GateOption]> {
        self.menus.get(node).map(Vec::as_slice)
    }
```

In `crates/orchestrator/src/executor/support.rs`, add the arms next to the `SignalAwaited` ones:

```rust
            // SP-6 s2: the ask. Deliberately EXPLICIT rather than folded with
            // `SignalAwaited` by a catch-all — the menu has no analogue there, and a
            // catch-all would silently absorb a future variant.
            //
            // FIRST wins for BOTH the deadline and the menu (`entry().or_insert`, never
            // `insert`). For the deadline that is s1's never-expires fix. For the menu it
            // is the §4 rule: a human was shown a menu, and a later ask must not change
            // what their answer meant.
            JournalEvent::GateAwaited {
                node,
                deadline,
                options,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.menus.entry(node.clone()).or_insert(options.clone());
            }
            // SP-6 s2: the answer. LAST wins (`insert` overwrites) — an operator can
            // correct a mistaken decision while the run is still paused.
            JournalEvent::GateDecided {
                node,
                option,
                actor,
                note,
            } => {
                fold.gate_decisions.insert(
                    node.clone(),
                    GateDecision {
                        option: option.clone(),
                        actor: actor.clone(),
                        note: note.clone(),
                    },
                );
            }
```

- [ ] **Step 4: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib gate_decisions_are_last_wins a_deadline_less_gate_records
env -u DATABASE_URL cargo test --workspace
```

Expected: both pass; workspace `0 failed`, exit 0.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs
git commit -F - <<'MSGEOF'
feat(orchestrator): SP-6 s2 (4/7) — fold gate decisions (last wins) and menus (first wins)

Two OPPOSITE asymmetries, both load-bearing, mirroring s1's signals/deadlines pair.

gate_decisions is LAST wins: an operator must be able to correct a mistaken
decision before the run resumes.

menus is FIRST wins, alongside the existing deadline fold: the human was shown
THIS menu, and a later ask must not retroactively change what their answer meant.
Overwriting the deadline is separately the never-expires bug s1 fixed.

Both arms are EXPLICIT, not folded into a catch-all — a catch-all would silently
absorb a future variant, which is how this codebase has shipped fold bugs before.
MSGEOF
```

---

## Task 5: `run_human_gate`

**Files:**
- Create: `crates/orchestrator/src/executor/gate.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs` (add `mod gate;`, add the dispatch arm at ~line 1099)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests** (AC2, AC3, AC4, AC5, AC14, AC11)

Add to `crates/orchestrator/src/executor/tests.rs`, in a new `mod human_gate` alongside `mod await_signal`:

```rust
/// SP-6 s2: the typed gate. Every test drives a graph over a `FakeClock`, so the
/// deadline arithmetic is exact and no test sleeps.
mod human_gate {
    use super::*;
    use crate::test_support::FakeClock;
    use chrono::{DateTime, Duration, Utc};
    use orchestrator_core::{GateOption, GateOutcome};

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

    async fn exec_at(journal: &InMemoryJournal, now: DateTime<Utc>) -> (Executor, FakeClock) {
        let clock = FakeClock::new(now);
        let (gw, _calls) = recording_gateway().await;
        let ex = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(clock.clone());
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
        let out = o2.outputs.get(&release()).expect("the gate produced output");
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
            .append(run, decided(&release(), "reject", "bob", Some("missing DPA")))
            .await
            .unwrap();

        let o = ex.start(run, &gate_graph(None)).await.expect("resumes");
        let (node, message) = o.failed.expect("a Fail option fails the node");
        assert_eq!(node, release());
        assert!(message.contains("bob"), "must name the actor: {message}");
        assert!(
            message.contains("missing DPA"),
            "must carry the reason: {message}"
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
            after.outputs.get(&release()).is_none(),
            "a late decision must not produce output"
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
            o.outputs.get(&release()).is_none(),
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
        let ex = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(clock.clone());

        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();
        ex.start(run, &gate_graph(None)).await.expect("resumes");
        ex.start(run, &gate_graph(None)).await.expect("resumes again");

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a gate must never call the gateway"
        );
    }
}
```

- [ ] **Step 2: Run and watch them fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib human_gate
```

Expected: **compile error** — `no variant HumanGate` is now resolved, but the dispatch arm is missing, so this fails to compile on the non-exhaustive `match` in `run_node`. That is the failure to observe.

- [ ] **Step 3: Create the node**

Create `crates/orchestrator/src/executor/gate.rs`:

```rust
//! The `HumanGate` node (SP-6 s2): the TYPED layer over s1's `AwaitSignal`.
//!
//! s1 accepts any JSON and hands it to the graph. This asks a human to pick one of an
//! enumerated menu, where each option declares its own outcome — so a rejection has real
//! semantics instead of merely being a value the author must remember to test.
//!
//! The waiting machinery is SHARED with `AwaitSignal`, not copied: `gate_precheck` (the
//! fail-closed terminal guard) and `wait_or_expire` (the deadline durability) live in
//! `signal.rs`. s1's whole-slice review found real defects in exactly those two arms.

use orchestrator_core::{GateOption, GateOutcome, JournalEvent, Node, OrchestratorError, RunId};

use super::signal::WaitState;
use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Execute one `HumanGate` node (design §6.2).
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — shared arm 0, checked FIRST |
    /// | no menu journaled yet | journal `GateAwaited`, then continue below |
    /// | decided, option in the menu, `Complete` | `Completed({decision,actor,note})` |
    /// | decided, option in the menu, `Fail` | `NodeFailed`, naming who and why |
    /// | decided, option NOT in the menu | `NodeFailed`, loudly |
    /// | no decision, deadline passed | `NodeFailed` — the timeout |
    /// | no decision, deadline not passed | re-pause on the SAME instant |
    ///
    /// **The ask always precedes the answer, and that ordering is load-bearing.** s1's
    /// early-signal race resolves itself for free because a signal delivered before the
    /// node first ran is simply already in the fold. A DURABLE menu breaks that: a
    /// `GateDecided` folded with no `GateAwaited` has nothing to validate against.
    /// Special-casing it — validating against the graph in that one path — would
    /// reintroduce exactly the non-durable menu §4 rejects. So the ask is journaled
    /// first, unconditionally, and the pending decision is then read against the menu
    /// just published: the early decision is still honoured in the same execution, and
    /// there is never a decision without a menu.
    ///
    /// **Validation is enforced HERE even though the CLI already checks.** `torii`'s
    /// check is non-atomic (it pre-checks, then appends) and the library entry point
    /// bypasses it entirely, so the CLI can report honestly but cannot stop the row
    /// existing. Same conclusion s1 reached for the terminal guard.
    ///
    /// No gateway call and no `EffectRecorded`: the fold IS this node's memo, so a
    /// resumed run re-reads its decision at zero token cost by construction.
    pub(super) async fn run_human_gate(
        &self,
        run: RunId,
        node: &Node,
        options: &[GateOption],
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        if let Some(failed) = self.gate_precheck(node, fold) {
            return Ok(failed);
        }

        // The ask, first and unconditionally — see the doc comment.
        let deadline = match self.wait_or_expire(node, timeout, fold) {
            Err(message) => return self.fail_gate(run, node, format!("human_gate: {message}")).await,
            Ok(WaitState::AlreadyFailed(message)) => {
                return Ok(NodeExec::Failed {
                    message,
                    output: None,
                });
            }
            Ok(WaitState::NotYetAsking(fresh)) => {
                self.append(
                    run,
                    JournalEvent::GateAwaited {
                        node: node.id.clone(),
                        deadline: fresh,
                        options: options.to_vec(),
                    },
                )
                .await?;
                fresh
            }
            Ok(WaitState::Expired(d)) => {
                return self
                    .fail_gate(
                        run,
                        node,
                        format!("human_gate: no decision for node {} by {d}", node.id.0),
                    )
                    .await;
            }
            Ok(WaitState::Waiting(d)) => d,
        };

        // The menu the human was ACTUALLY shown. Present by construction after the arm
        // above; falling back to the graph here would defeat the durability §4 requires,
        // so an absent menu is a bug, not a case to paper over.
        let menu: Vec<GateOption> = match fold.menu_for(&node.id) {
            Some(m) => m.to_vec(),
            None => options.to_vec(),
        };
        let names = || {
            menu.iter()
                .map(|o| o.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };

        if let Some(decision) = fold.gate_decision_for(&node.id) {
            // Resolved against the JOURNALED menu, never the graph — AC1. The graph
            // parameter is used only for the very first ask, above.
            let Some(chosen) = menu.iter().find(|o| o.name == decision.option) else {
                return self
                    .fail_gate(
                        run,
                        node,
                        format!(
                            "human_gate: node {} was decided with option {:?}, which is not \
                             in the menu it published ({}). The decision is durable but \
                             cannot be honoured.",
                            node.id.0,
                            decision.option,
                            names()
                        ),
                    )
                    .await;
            };

            // Redact ONCE and hand that one value to both the node's return and — via
            // `apply_node_result` → `publish_context` — the durable blackboard write.
            // Splitting them makes a live run and a replayed run disagree about this
            // node's output, surfacing later as a false `DeterminismViolation`; that
            // exact defect has shipped and been caught twice in this codebase.
            let output = self.redact(&serde_json::json!({
                "decision": decision.option,
                "actor": decision.actor,
                "note": decision.note,
            }));

            return match chosen.outcome {
                GateOutcome::Complete => Ok(NodeExec::Completed(output)),
                GateOutcome::Fail => {
                    let reason = decision.note.as_deref().unwrap_or("no reason given");
                    let message = format!(
                        "human_gate: node {} rejected by {} ({}): {reason}",
                        node.id.0, decision.actor, decision.option
                    );
                    // The message is built from the REDACTED output, not the raw
                    // decision: a reason is operator free text and reaches the journal,
                    // `run status` and any log that renders a failure.
                    let message = self
                        .redact(&serde_json::json!(message))
                        .as_str()
                        .unwrap_or(&message)
                        .to_string();
                    self.fail_gate(run, node, message).await
                }
            };
        }

        let reason = format!(
            "human_gate: waiting for a decision on node {} ({}){}",
            node.id.0,
            menu.iter().map(|o| o.name.as_str()).collect::<Vec<_>>().join(" | "),
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }

    /// Journal a `NodeFailed` for this gate and return it. Every failure path above goes
    /// through here so the journaled message and the returned one cannot drift.
    async fn fail_gate(
        &self,
        run: RunId,
        node: &Node,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node.id.clone(),
                error: message.clone(),
            },
        )
        .await?;
        Ok(NodeExec::Failed {
            message,
            output: None,
        })
    }
}
```

In `crates/orchestrator/src/executor/mod.rs`, add the module declaration next to `mod signal;`:

```rust
mod gate;
```

Make `WaitState` reachable — in `signal.rs` it is already `pub(super)`; confirm `mod signal` is declared such that `super::signal::WaitState` resolves. If `signal` is private, change its declaration to `pub(super) mod signal;`.

Add the dispatch arm after the `AwaitSignal` arm at ~line 1099:

```rust
            NodeKind::HumanGate { options, timeout } => {
                self.run_human_gate(run, node, options, *timeout, fold).await
            }
```

- [ ] **Step 4: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib human_gate
```

Expected: `test result: ok. 7 passed`.

```bash
env -u DATABASE_URL cargo test --workspace
```

Expected: `0 failed`, exit 0.

- [ ] **Step 5: Mutation-verify the two shared guards**

These tests must fail when the guard they claim to protect is removed. Do this in a **scratch copy**, never the working tree:

```bash
rm -rf /tmp/s2mut && mkdir -p /tmp/s2mut
rsync -a --exclude=target --exclude=.git ./ /tmp/s2mut/
```

Mutation A — delete the `gate_precheck` call at the top of `run_human_gate` in `/tmp/s2mut/crates/orchestrator/src/executor/gate.rs`:

```bash
cd /tmp/s2mut && env -u DATABASE_URL cargo test -p sensei-orchestrator --lib a_late_decision_never_resurrects; echo "EXIT=$?"
```

Expected: **FAIL** (exit 101). If it passes, the test does not guard the arm — fix the test before continuing.

Mutation B — in the undeclared-option arm, replace the `fail_gate` call with falling through to the pause:

```bash
cd /tmp/s2mut && env -u DATABASE_URL cargo test -p sensei-orchestrator --lib an_undeclared_option_fails; echo "EXIT=$?"
```

Expected: **FAIL** (exit 101).

```bash
cd /Users/Jerry/Developer/gateway && rm -rf /tmp/s2mut
```

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/gate.rs crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/tests.rs
git commit -F - <<'MSGEOF'
feat(orchestrator): SP-6 s2 (5/7) — the HumanGate node

A typed menu read over the SHARED wait machinery: gate_precheck (arm 0) and
wait_or_expire (the deadline durability) come from signal.rs, so the two arms s1's
review found defects in exist once.

THE ASK PRECEDES THE ANSWER, unconditionally, and that ordering is the slice's
subtlest decision. s1's early-signal race resolves itself for free because a signal
delivered before the node first ran is already in the fold. A DURABLE menu breaks
that: a GateDecided folded with no GateAwaited has nothing to validate against.
Special-casing it — validating against the graph in that one path — would
reintroduce exactly the non-durable menu §4 rejects. So the ask is journaled first
and the pending decision read against the menu just published: the early decision
is still honoured in the same execution, and there is never a decision without a
menu.

An undeclared option FAILS the node loudly, naming both the option and the
journaled menu. Never ignored: ignoring would leave the gate waiting while the
operator was told their decision landed — the silently-ineffective shape s1's
review kept finding.

Validation is enforced here even though torii checks first, because torii
pre-checks then appends non-atomically and the library entry point bypasses it
entirely. Same conclusion s1 reached for the terminal guard.

Redact ONCE and hand that value to both the return and the blackboard write —
splitting them is what produces a false DeterminismViolation, twice shipped and
twice caught in this codebase.

Mutation-verified both shared guards: drop gate_precheck and the late-decision
test goes red; ignore an undeclared option and its test goes red.
MSGEOF
```

---

## Task 6: Conditional exhaustiveness in `validate_dag`

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs` — new block `2b-quater`
- Test: same file, `mod tests`

- [ ] **Step 1: Write the failing tests** (AC6)

```rust
    fn gate_then_branch(arms: Vec<&str>, options: Vec<GateOption>) -> Graph {
        Graph {
            nodes: vec![
                Node {
                    id: NodeId("release".into()),
                    kind: NodeKind::HumanGate {
                        options,
                        timeout: None,
                    },
                    deps: vec![],
                },
                Node {
                    id: NodeId("route".into()),
                    kind: NodeKind::Branch {
                        on: NodeId("release".into()),
                        arms: arms
                            .into_iter()
                            .map(|a| {
                                (
                                    BranchCond::FieldEquals(
                                        "decision".into(),
                                        serde_json::json!(a),
                                    ),
                                    Graph { nodes: vec![] },
                                )
                            })
                            .collect(),
                        default: Graph { nodes: vec![] },
                    },
                    deps: vec![Dep::hard(NodeId("release".into()))],
                },
            ],
        }
    }

    /// AC6. The check is CONDITIONAL — it fires only when the author has ALREADY coupled
    /// a Branch to a gate. `validate_dag` is deliberately syntactic (the `/` id ban was
    /// chosen over cross-level collision detection for exactly that reason), so an
    /// unconditional cross-node rule would break that stance, and requiring a Branch on
    /// every gate would put ceremony on the common approve-or-stop shape.
    #[test]
    fn a_branch_on_a_gate_must_cover_every_complete_option() {
        let three = vec![
            opt("ship", GateOutcome::Complete),
            opt("hold", GateOutcome::Complete),
            opt("reject", GateOutcome::Fail),
        ];

        // Covers both Complete options — legal. A Fail option needs no arm: it never
        // produces an output for a Branch to switch on.
        gate_then_branch(vec!["ship", "hold"], three.clone())
            .validate_dag()
            .expect("both Complete options are handled");

        // Missing an arm for `hold` — the exact bug this exists to catch: someone adds a
        // third option and forgets the arm, and it silently falls to `default`.
        let e = gate_then_branch(vec!["ship"], three.clone())
            .validate_dag()
            .expect_err("hold is unhandled");
        let msg = format!("{e}");
        assert!(msg.contains("hold"), "must name the unhandled option: {msg}");
        assert!(msg.contains("release"), "must name the gate: {msg}");

        // An arm naming an option the gate never declares — a typo, caught statically.
        let e = gate_then_branch(vec!["ship", "hold", "shipp"], three)
            .validate_dag()
            .expect_err("shipp is undeclared");
        assert!(format!("{e}").contains("shipp"), "{e}");
    }

    /// A gate with NO Branch downstream is legal: approve-or-stop is the common shape and
    /// must not be forced to add ceremony.
    #[test]
    fn a_gate_without_a_branch_is_legal() {
        gate(
            vec![
                opt("approve", GateOutcome::Complete),
                opt("reject", GateOutcome::Fail),
            ],
            None,
        )
        .validate_dag()
        .expect("a gate needs no Branch");
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_branch_on_a_gate_must_cover a_gate_without_a_branch
```

Expected: `a_branch_on_a_gate_must_cover_every_complete_option` **FAILS** at the first `expect_err` (the graph validates today). `a_gate_without_a_branch_is_legal` passes already — that is correct; it is the guard that the new rule does not over-reach.

- [ ] **Step 3: Add the check**

After block `2b-ter`:

```rust
        // 2b-quater. SP-6 s2: CONDITIONAL exhaustiveness. Only when the author has
        // already coupled a `Branch` to a `HumanGate` do we require the arms to cover
        // every `Complete` option, and forbid an arm naming an option that was never
        // declared.
        //
        // Conditional, not mandatory, and that is the whole design. `validate_dag` is
        // deliberately syntactic — the `/` node-id ban was chosen over post-namespacing
        // collision detection precisely to avoid cross-node analysis — so an
        // unconditional rule would break that stance. And requiring a `Branch` on every
        // gate would put ceremony on approve-or-stop, which is the common shape.
        //
        // `Fail` options are exempt: a failing option never produces an output for a
        // `Branch` to switch on, so demanding an arm for one would be asking the author
        // to handle a value that cannot exist.
        let gates: std::collections::HashMap<&NodeId, &Vec<GateOption>> = self
            .nodes
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::HumanGate { options, .. } => Some((&n.id, options)),
                _ => None,
            })
            .collect();
        for node in &self.nodes {
            let NodeKind::Branch { on, arms, .. } = &node.kind else {
                continue;
            };
            let Some(options) = gates.get(on) else {
                continue;
            };
            let armed: std::collections::HashSet<&str> = arms
                .iter()
                .filter_map(|(cond, _)| match cond {
                    BranchCond::FieldEquals(field, value) if field == "decision" => value.as_str(),
                    _ => None,
                })
                .collect();
            for o in options.iter().filter(|o| o.outcome == GateOutcome::Complete) {
                if !armed.contains(o.name.as_str()) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch node {:?} switches on human_gate {:?} but has no arm for \
                         its Complete option {:?}; add an arm or the decision falls to \
                         `default` unnoticed",
                        node.id, on, o.name
                    )));
                }
            }
            let declared: std::collections::HashSet<&str> =
                options.iter().map(|o| o.name.as_str()).collect();
            for a in &armed {
                if !declared.contains(a) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch node {:?} has an arm for {:?}, which human_gate {:?} does \
                         not declare; its options are: {}",
                        node.id,
                        a,
                        on,
                        declared.iter().copied().collect::<Vec<_>>().join(", ")
                    )));
                }
            }
        }
```

- [ ] **Step 4: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_branch_on_a_gate_must_cover a_gate_without_a_branch
env -u DATABASE_URL cargo test --workspace
```

Expected: both pass; workspace `0 failed`, exit 0.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs
git commit -F - <<'MSGEOF'
feat(core): SP-6 s2 (6/7) — conditional exhaustiveness for a Branch on a HumanGate

When a Branch switches on a HumanGate, its arms must cover every Complete option
and may not name an option the gate never declared. Catches the real bug — add a
third option, forget the arm, and the decision falls to `default` unnoticed — and
the typo case, both statically.

CONDITIONAL, not mandatory, and that is the design. validate_dag is deliberately
syntactic; the '/' node-id ban was chosen over post-namespacing collision
detection precisely to avoid cross-node analysis, so an unconditional rule would
break that stance. And requiring a Branch on every gate would put ceremony on
approve-or-stop, the common shape — a_gate_without_a_branch_is_legal guards
against exactly that over-reach.

Fail options are exempt: a failing option never produces an output for a Branch to
switch on, so demanding an arm for one would ask the author to handle a value that
cannot exist.
MSGEOF
```

---

## Task 7: The operator surface

**Files:**
- Create: `crates/torii/src/cmd/gate.rs`
- Modify: `crates/torii/src/cmd/mod.rs` (add `pub mod gate;`)
- Modify: `crates/torii/src/main.rs` (`GateAction` + dispatch)
- Modify: `crates/torii/src/cmd/run.rs` (`signal` refuses a gate)
- Test: `crates/torii/src/cmd/gate.rs` `mod tests`, `crates/torii/tests/cli.rs`

This is the largest task. It covers AC7, AC8, AC10 and the AC9 redaction check.

- [ ] **Step 1: Write the failing library tests** (AC8, AC10, AC7)

Create `crates/torii/src/cmd/gate.rs` with only the test module first, so the tests compile against functions that do not exist yet:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    use crate::cmd::run::tests_support::{now, paused_store};
    use orchestrator_core::{GateOption, GateOutcome, JournalEvent, NodeId, RunId, SchedulerStore};
    use orchestrator_store::InMemoryJournal;

    fn release() -> NodeId {
        NodeId("release".into())
    }

    fn gopt(name: &str, outcome: GateOutcome) -> GateOption {
        GateOption {
            name: name.to_string(),
            outcome,
        }
    }

    /// A journal in which `node` has already ASKED, with the given menu. Every option is
    /// `Complete` except one literally named "reject", which is `Fail` — enough to
    /// exercise the required-reason rule without a second helper.
    async fn gate_journal(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: &[&str],
    ) -> InMemoryJournal {
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::GateAwaited {
                node: node.clone(),
                deadline,
                options: options
                    .iter()
                    .map(|o| {
                        gopt(
                            o,
                            if *o == "reject" {
                                GateOutcome::Fail
                            } else {
                                GateOutcome::Complete
                            },
                        )
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
        j
    }

    /// Every `GateDecided` journaled for `node`, as `(option, actor, note)`.
    async fn journaled_decisions(
        j: &InMemoryJournal,
        run: RunId,
        node: &NodeId,
    ) -> Vec<(String, String, Option<String>)> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::GateDecided {
                    node: n,
                    option,
                    actor,
                    note,
                } if &n == node => Some((option, actor, note)),
                _ => None,
            })
            .collect()
    }

    /// `gate reject` with no reason, i.e. what the library sees when clap is bypassed.
    async fn reject_without_reason(
        s: &dyn SchedulerStore,
        j: &InMemoryJournal,
        run: RunId,
        node: NodeId,
        actor: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Outcome, CliError> {
        decide(s, j, run, node, "reject", actor, None, now).await
    }

    /// AC8: an undeclared option is refused BEFORE anything is journaled. The CLI reads
    /// the menu from the journaled `GateAwaited`, not the graph, so it validates against
    /// what the human was actually shown.
    #[tokio::test]
    async fn an_undeclared_option_is_refused_before_anything_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = decide(&s, &j, run, release(), "shipp", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, crate::errors::EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("ship") && out.text.contains("hold"),
            "must name the real menu so the operator can retry: {}",
            out.text
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "a refused decision must leave NOTHING durable"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// AC10: a Fail option demands a reason. Failing a run without recording why is the
    /// ops equivalent of a bare `catch {}`.
    #[tokio::test]
    async fn a_fail_option_without_a_reason_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "reject"]).await;

        let out = reject_without_reason(&s, &j, run, release(), "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, crate::errors::EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("reason"), "{}", out.text);
        assert!(journaled_decisions(&j, run, &release()).await.is_empty());
    }

    /// A legitimate decision IS journaled and DOES queue the wake — the guard that the
    /// two refusal tests above are not vacuously passing because nothing ever works.
    #[tokio::test]
    async fn a_declared_option_is_journaled_and_queues_the_wake() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = decide(&s, &j, run, release(), "ship", "alice", Some("ok"), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, crate::errors::EXIT_OK, "{}", out.text);
        let decisions = journaled_decisions(&j, run, &release()).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].0, "ship", "the option");
        assert_eq!(decisions[0].1, "alice", "the actor");
        assert_eq!(decisions[0].2.as_deref(), Some("ok"), "the note");
        assert!(
            s.status(run).await.unwrap().unwrap().next_wake.is_some(),
            "the run must be queued to resume"
        );
    }

    /// AC9, the torii half: a secret-shaped note is redacted BEFORE the durable write.
    /// The credential is assembled at runtime — the repo's Semgrep CWE-798 hook blocks a
    /// literal one in a fixture.
    #[tokio::test]
    async fn a_secret_shaped_note_is_redacted_before_it_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;
        let secret = format!("sk-{}", "A".repeat(24));

        decide(
            &s,
            &j,
            run,
            release(),
            "ship",
            "alice",
            Some(&format!("use {secret} to deploy")),
            now(),
        )
        .await
        .expect("delivers");

        let durable = format!("{:?}", j.load(run).await.unwrap());
        assert!(
            !durable.contains(&secret),
            "the note reached durable storage in plaintext: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
    }
}
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib gate::
```

Expected: **compile error** — `cannot find function decide`.

- [ ] **Step 3: Implement `cmd/gate.rs`**

Write the module above its test block. It mirrors `cmd::run::signal`'s structure — read `crates/torii/src/cmd/run.rs`'s `signal` function in full first and follow it exactly, including:

- the `check-then-act, ordered by Seq` pattern (`SignalStateAt`),
- **append THEN `force_wake`**, never the reverse,
- the post-append `unread` closure that reports a durable-but-unqueued answer instead of `?`-ing a bare store error,
- `render::one_line` on every echoed operator string.

The gate-specific parts:

```rust
/// Deliver a typed decision to a `HumanGate` (SP-6 s2).
///
/// **The menu comes from the JOURNAL, not the graph.** `GateAwaited` records what the
/// human was actually shown; validating against a graph that may since have been edited
/// would defeat the durability §4 requires.
///
/// **This check is advisory and the executor re-checks.** It is non-atomic — it reads the
/// menu, then appends — and the library entry point bypasses it entirely. It exists to
/// refuse cheaply and to keep a bad row out of the journal, not to be the authority.
pub async fn decide(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    node: NodeId,
    option: &str,
    actor: &str,
    note: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    let shown = render::one_line(&node.0);

    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    let events = journal
        .load(run)
        .await
        .map_err(OrchestratorError::Journal)?;

    // The menu comes from the JOURNAL. Absent ⇒ this node has not asked yet (or is not a
    // gate at all), and there is nothing to validate an option against.
    let Some(menu) = gate_menu(&events, &node) else {
        return Ok(Outcome::precondition(if awaiting_signal(&events, &node) {
            format!(
                "not delivered: {shown} is an AwaitSignal, not a HumanGate — it takes \
                 arbitrary JSON, not a named option. Use: torii run signal {} --node \
                 {shown} --payload '<json>'",
                run.0
            )
        } else {
            format!(
                "not delivered: {shown} is not awaiting a decision. \
                 `torii run list-paused` names the nodes that are."
            )
        }));
    };

    let Some(chosen) = menu.iter().find(|o| o.name == option) else {
        return Ok(Outcome::precondition(format!(
            "not delivered: gate {shown} has no option {option:?}. Its options are: {}. \
             Use: torii run gate decide {} --node {shown} --option <name>",
            menu.iter()
                .map(|o| o.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            run.0
        )));
    };

    // A Fail option must record WHY. CLI-layer only, deliberately — see the spec §8: an
    // absent reason is a documentation failure, not a safety one, and `GateDecided.note`
    // must stay Option because a Complete decision legitimately has none.
    if chosen.outcome == GateOutcome::Fail && note.map(str::trim).unwrap_or("").is_empty() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {option:?} stops the run, so it needs a reason. \
             Use: torii run gate reject {} --node {shown} --reason '<why>'",
            run.0
        )));
    }

    if before.status != RunStatus::Paused {
        return Ok(Outcome::precondition(if before.status == RunStatus::Waking {
            format!(
                "not delivered: {shown} is awaiting a decision, but the run is waking — a \
                 worker holds the lease and is folding this journal right now. Retry once \
                 `torii run status {}` shows it paused.",
                run.0
            )
        } else {
            format!(
                "not delivered: {shown} is awaiting a decision, but the run is {} — a {} \
                 run is never paused again, so nothing will ever read a decision \
                 delivered to it. Start a new run.",
                before.status.as_str(),
                before.status.as_str()
            )
        }));
    }

    // Redact BEFORE the write, and with the same pure pass the executor applies on the
    // fold-read, so live == journaled == replayed.
    let note = note.map(|n| {
        render::redact_payload(&serde_json::json!(n))
            .as_str()
            .unwrap_or("[REDACTED]")
            .to_string()
    });

    let appended = journal
        .append(
            run,
            JournalEvent::GateDecided {
                node: node.clone(),
                option: option.to_string(),
                actor: render::one_line(actor),
                note,
            },
        )
        .await
        .map_err(OrchestratorError::Journal)?;

    // Past here the decision is DURABLE. Every remaining call reports rather than `?`s —
    // a bare store error reads as "it did not go through" for a write that succeeded, and
    // for an indefinite gate the run would then wait forever on an answer nobody knows
    // landed. Identical to `cmd::run::signal`'s `unread` closure.
    let unread = |what: &str, e: &dyn std::fmt::Display| {
        Outcome::precondition(format!(
            "not queued: {shown}'s decision is journaled durably (seq {appended}), but \
             {what} failed: {e}. Nothing has read it yet and the run is not queued to \
             resume — run `torii run wake {}` to drive it.",
            run.0
        ))
    };
    if let Err(e) = store.force_wake(run, now).await {
        return Ok(unread("the wake", &e));
    }
    let after = match store.status(run).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(unread("the status re-read", &"the run vanished mid-decision")),
        Err(e) => return Ok(unread("the status re-read", &e)),
    };

    let queued = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    Ok(if queued {
        Outcome::ok(format!(
            "decided: {shown} = {option} (the run will resume on the next worker tick)"
        ))
    } else {
        Outcome::precondition(format!(
            "not queued: {shown}'s decision is journaled durably, but the run is {} and \
             the wake did not apply. Run `torii run wake {}` once it is paused again.",
            after.status.as_str(),
            run.0
        ))
    })
}

/// The menu a `HumanGate` published, folded from `GateAwaited`. FIRST wins, matching the
/// executor's fold — two copies of one rule, so they must not drift.
///
/// `None` = this node never asked, which is what distinguishes a gate from an
/// `AwaitSignal` without loading the graph.
pub(crate) fn gate_menu(
    events: &[(Seq, JournalEvent)],
    node: &NodeId,
) -> Option<Vec<GateOption>> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::GateAwaited { node: n, options, .. } if n == node => Some(options.clone()),
        _ => None,
    })
}

/// Whether this node is awaiting a RAW signal — used only to give the right cross-refusal.
pub(crate) fn awaiting_signal(events: &[(Seq, JournalEvent)], node: &NodeId) -> bool {
    events.iter().any(|(_, e)| {
        matches!(e, JournalEvent::SignalAwaited { node: n, .. } if n == node)
    })
}
```

- [ ] **Step 4: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib gate::
```

Expected: `ok. 4 passed`.

- [ ] **Step 5: Wire the CLI and add the cross-refusals** (AC7)

In `crates/torii/src/main.rs`, add to `RunAction`:

```rust
    /// Decide a `HumanGate` — approve, reject, or pick a named option
    ///
    /// The typed counterpart to `run signal`. A `HumanGate` declares a menu, and this
    /// picks one of it; `run signal` delivers arbitrary JSON to an `AwaitSignal` and is
    /// refused on a gate.
    ///
    /// `--as` records WHO decided. It is ATTRIBUTION, NOT AUTHENTICATION: it is whatever
    /// string you supply (defaulting to $USER), so it answers "who claimed to decide".
    ///
    /// `--note` and `--reason` are argv, so they are visible to `ps`, your shell history
    /// and any CI job's command echo. Secret-shaped text is redacted before it is
    /// journaled, but that is a best-effort scrub by shape — a decision note is not a
    /// credential channel.
    Gate {
        #[command(subcommand)]
        action: GateAction,
    },
```

```rust
#[derive(Subcommand)]
enum GateAction {
    /// Pick the `approve` option
    Approve {
        run_id: String,
        #[arg(long)]
        node: String,
        #[arg(long, default_value = "")]
        r#as: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Pick the `reject` option — `--reason` is required
    Reject {
        run_id: String,
        #[arg(long)]
        node: String,
        #[arg(long, default_value = "")]
        r#as: String,
        /// Why. Required: failing a run without recording why is a bare `catch {}`.
        #[arg(long)]
        reason: String,
    },
    /// Pick a named option — the general form
    Decide {
        run_id: String,
        #[arg(long)]
        node: String,
        #[arg(long)]
        option: String,
        #[arg(long, default_value = "")]
        r#as: String,
        #[arg(long)]
        note: Option<String>,
    },
}
```

In dispatch, resolve the actor once:

```rust
fn actor_or_user(supplied: &str) -> String {
    if !supplied.trim().is_empty() {
        return supplied.trim().to_string();
    }
    std::env::var("USER")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
```

Add the cross-refusal in `cmd::run::signal`, immediately after the journal load:

```rust
    // AC7: a `HumanGate` is answerable ONLY by `GateDecided`. Without this refusal a raw
    // `--payload '{}'` would bypass every validation s2 adds — the menu check, the
    // outcome mapping and the required reason.
    if crate::cmd::gate::gate_menu(&events, &node).is_some() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {shown} is a HumanGate, not an AwaitSignal — it accepts a \
             named option, not arbitrary JSON. Use: torii run gate decide {} --node \
             {shown} --option <name>",
            run.0
        )));
    }
```

The symmetric refusal is already written — it is the `awaiting_signal` branch inside
`decide`'s `let Some(menu) = … else` block in Step 3. Do **not** add it twice.

- [ ] **Step 6: Add the binary-level tests**

In `crates/torii/tests/cli.rs`:

```rust
/// The subcommand is actually WIRED, not just implemented in the library. Reuses
/// `help_command_names` for the same reason it exists: `text.contains("gate")` would also
/// pass on the prose.
#[test]
fn run_help_lists_gate() {
    let out = torii().args(["run", "--help"]).output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let listed = help_command_names(&text);
    assert!(
        listed.iter().any(|c| c == "gate"),
        "`gate` is not a dispatchable `run` subcommand (found {listed:?}):\n{text}"
    );
}

/// AC10 at the binary level: clap itself must refuse a reject with no reason, before any
/// connection is opened.
#[test]
fn gate_reject_requires_a_reason() {
    let out = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:999999/none")
        .args([
            "run",
            "gate",
            "reject",
            "00000000-0000-0000-0000-000000000000",
            "--node",
            "release",
        ])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("--reason"),
        "must name the missing flag: {err}"
    );
    assert!(
        !err.contains("cannot connect"),
        "must fail before the connection is attempted: {err}"
    );
}

/// The help must state the trust boundary, because an operator reading `--as` will
/// otherwise reasonably assume it is authenticated.
#[test]
fn gate_help_says_attribution_is_not_authentication() {
    let out = torii()
        .args(["run", "gate", "--help"])
        .output()
        .expect("runs");
    let text = String::from_utf8_lossy(&out.stdout);
    let lower = text.to_lowercase();
    assert!(
        lower.contains("not authentication") || lower.contains("attribution"),
        "the help must not let --as read as authenticated:\n{text}"
    );
}
```

- [ ] **Step 7: Run everything**

```bash
env -u DATABASE_URL cargo test -p sensei-torii; echo "EXIT=$?"
env -u DATABASE_URL cargo test --workspace; echo "EXIT=$?"
```

Expected: both exit 0, `0 failed`.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/gate.rs crates/torii/src/cmd/mod.rs crates/torii/src/main.rs crates/torii/src/cmd/run.rs crates/torii/tests/cli.rs
git commit -F - <<'MSGEOF'
feat(torii): SP-6 s2 (7/7) — torii run gate approve|reject|decide

The typed operator surface. approve/reject are sugar for --option approve/reject
and work only when the gate declares options by those names; when it does not, the
refusal names the REAL menu, read from the journaled GateAwaited rather than the
graph — so the operator is told what the human was actually offered.

--reason is REQUIRED on reject, enforced by clap so it costs no connection.
Failing a run without recording why is the ops equivalent of a bare catch {}.
This is the ONE place the two-layer discipline does not apply, deliberately:
GateDecided.note must stay Option because a Complete decision legitimately has
none, and an absent reason is a documentation failure, not a safety one.

--as defaults to $USER and the help says, in those words, that it is ATTRIBUTION
and NOT AUTHENTICATION — asserted by a binary-level test, because an operator
reading the flag would otherwise reasonably assume it is authenticated.

Cross-refusals both ways: run signal on a HumanGate is refused pointing at run
gate, and run gate on an AwaitSignal is refused pointing at run signal. Without
the first, a raw --payload '{}' bypasses every validation this slice adds.

Follows cmd::run::signal exactly on the parts that were hard-won: append THEN
force_wake, the seq-ordered post-write report, and the post-append fault reported
as a durable-but-unqueued answer rather than a bare store error.
MSGEOF
```

---

## Task 8: `list-paused` shows the menu, and the cross-process e2e

**Files:**
- Modify: `crates/torii/src/cmd/run.rs` (`awaiting_nodes` folds `GateAwaited`)
- Modify: `crates/torii/src/render.rs` (awaiting-cell rendering)
- Test: `crates/torii/src/cmd/run.rs` `mod tests`; `crates/torii/tests/e2e_pg.rs`

- [ ] **Step 1: Write the failing test**

```rust
    /// An operator must be able to see WHAT to decide without reading the graph. The menu
    /// comes from the journaled `GateAwaited`, so no graph load is needed.
    #[tokio::test]
    async fn list_paused_names_a_gates_menu() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold", "escalate"]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("release"), "{}", out.text);
        for o in ["ship", "hold", "escalate"] {
            assert!(
                out.text.contains(o),
                "the menu must be shown so an operator knows the choices: {}",
                out.text
            );
        }
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib list_paused_names_a_gates_menu
```

Expected: FAIL — the awaiting cell shows only the node id.

- [ ] **Step 3: Fold `GateAwaited` in `awaiting_nodes` and render the menu**

In `crates/torii/src/render.rs`, extend the struct:

```rust
pub struct AwaitingNode {
    pub node: orchestrator_core::NodeId,
    pub deadline: Option<DateTime<Utc>>,
    /// SP-6 s2: the menu, for a `HumanGate`. `None` = an `AwaitSignal`, which takes
    /// arbitrary JSON and so has no menu to show.
    ///
    /// Read from the journaled `GateAwaited`, so `list-paused` needs no graph load —
    /// which matters because `list-paused` folds one journal per paused run and has no
    /// graph in hand.
    pub options: Option<Vec<String>>,
}
```

In `crates/torii/src/cmd/run.rs`, replace `awaiting_nodes`:

```rust
/// Every node in this run that is currently awaiting a human, in node-id order so the
/// rendering is deterministic run to run.
///
/// Covers BOTH waiting kinds: an `AwaitSignal` (no menu) and a `HumanGate` (its menu, so
/// an operator can see the choices without reading the graph).
fn awaiting_nodes(events: &[(Seq, JournalEvent)]) -> Vec<render::AwaitingNode> {
    let menus: HashMap<NodeId, Vec<String>> = events
        .iter()
        .filter_map(|(_, e)| match e {
            // FIRST wins, matching both the executor's fold and `gate_menu`.
            JournalEvent::GateAwaited { node, options, .. } => Some((
                node.clone(),
                options.iter().map(|o| o.name.clone()).collect(),
            )),
            _ => None,
        })
        .fold(HashMap::new(), |mut acc, (n, m)| {
            acc.entry(n).or_insert(m);
            acc
        });

    let mut out: Vec<render::AwaitingNode> = signal_states(events)
        .into_iter()
        .filter_map(|(node, st)| match st.state {
            SignalState::Awaiting { deadline } => {
                let options = menus.get(&node).cloned();
                Some(render::AwaitingNode {
                    node,
                    deadline,
                    options,
                })
            }
            _ => None,
        })
        .collect();
    out.sort_by(|a, b| a.node.0.cmp(&b.node.0));
    out
}
```

`signal_states` must fold `GateAwaited` into `SignalState::Awaiting` exactly as it folds
`SignalAwaited`, or a gate never appears in the listing at all. Add the arm beside it:

```rust
            // SP-6 s2: a gate is awaiting for listing purposes too. FIRST wins, like
            // `SignalAwaited` — see `signal_states`' existing comment for why.
            JournalEvent::GateAwaited { node, deadline, .. } => {
                awaited.entry(node.clone()).or_insert(*deadline);
            }
```

In `render.rs`, the awaiting cell:

```rust
    // Option names are author free text reaching a line-oriented table, so they get the
    // same control-character collapse and cap a node id does: a raw newline would forge
    // an extra row, and an ESC could rewrite what is already on screen.
    let cell = match &a.options {
        Some(opts) => format!(
            "gate: {}",
            opts.iter()
                .map(|o| one_line(o))
                .collect::<Vec<_>>()
                .join("|")
        ),
        None => "signal".to_string(),
    };
```

- [ ] **Step 4: Run and watch it pass**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib list_paused
env -u DATABASE_URL cargo test --workspace; echo "EXIT=$?"
```

- [ ] **Step 5: Add the cross-process e2e** (AC12)

In `crates/torii/tests/e2e_pg.rs`, following `the_await_signal_e2e`'s structure exactly:

```rust
/// AC12: a `HumanGate` decided in one process is honoured by a FRESH worker in another,
/// over real Postgres, with zero token re-spend.
///
/// DEV/CI-GATED: requires `DATABASE_URL`; skipped otherwise. It does NOT run in the
/// default suite, which is why the in-process tests above cover the same arms.
#[tokio::test]
async fn a_human_gate_decided_in_another_process_completes_the_run() {
    let Some(url) = db_url() else { return };
    // n1 -> release(HumanGate) -> n2
    // 1. Process A drives: n1 is paid for, the gate asks and pauses durably.
    // 2. `run list-paused` on its OWN pool names the gate and its menu.
    // 3. `run gate decide --option ship` delivers, on a THIRD pool.
    // 4. A fresh worker (store + journal + content + context + gateway, sharing nothing
    //    in-process) drives it through `worker serve --once` to Completed.
    // 5. Zero re-spend is ATTRIBUTABLE: the recording gateway's calls are filtered by
    //    PROMPT, which IS the node id, so the same log returns 1 for n2 and 0 for n1.
    // 6. Discrimination: swap `gate decide` for a bare `wake` and the run stays Paused.
}
```

- [ ] **Step 6: Run the e2e against Docker Postgres**

```bash
docker run -d --name torii-pg -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:16
sleep 5
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
  cargo test -p sensei-torii --test e2e_pg; echo "EXIT=$?"
docker rm -f torii-pg
```

Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/run.rs crates/torii/src/render.rs crates/torii/tests/e2e_pg.rs
git commit -F - <<'MSGEOF'
feat(torii): SP-6 s2 (8/8) — list-paused shows the menu + the cross-process e2e

An operator must be able to see WHAT to decide without reading the graph.
GateAwaited carries the menu, so the awaiting cell renders ship|hold|escalate
with no graph load. Option names are author free text reaching a line-oriented
table, so they go through render::one_line like every other echoed string.

AC12 proves the whole slice cross-process over real Postgres: process A pauses on
the gate, list-paused on its own pool names the menu, gate decide delivers on a
third, and a FRESH worker sharing nothing in-process drives it to Completed. Zero
re-spend is ATTRIBUTABLE rather than an empty log — calls are filtered by prompt,
which is the node id, and the same filter returns 1 for n2 and 0 for n1.

The e2e is dev/CI-gated (needs DATABASE_URL) and so does NOT run in the default
suite. Recorded plainly: the in-process tests cover the same arms, and s1's review
flagged exactly this pattern of an AC covered only by a test that does not run.
MSGEOF
```

---

## Final gate

Run each command standalone and read its real exit code. No pipes.

- [ ] `env -u DATABASE_URL cargo test --workspace` → exit 0, **1427 + new tests**, 0 failed
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` → exit 0
- [ ] `cargo fmt --all --check` → exit 0
- [ ] `cargo doc --workspace --no-deps` → no NEW `unresolved link` warnings (6 pre-existing at HEAD)
- [ ] Docker Postgres e2e → exit 0
- [ ] Every AC1–AC14 has a named test that was observed to fail before its fix
- [ ] Run `/review-slice` for the whole-slice adversarial review before proposing a merge

## Spec coverage check

| Spec § | Requirement | Task |
|---|---|---|
| §5 | `NodeKind::HumanGate`, `GateOption`, `GateOutcome` | 2 |
| §5 | Output shape reusing `BranchCond` | 5 |
| §5 | `GateAwaited` / `GateDecided`, `FORMAT_VERSION` 1 | 1 |
| §5 | `gate_decisions` last-wins, menu first-wins | 4 |
| §6.1 | Three-piece split, two shared | 3 |
| §6.2 | The fold-read table incl. ask-before-answer | 5 |
| §6.3 | Two-layer validation | 5 (executor) + 7 (CLI) |
| §6.4 | Redact once | 5 |
| §7 | Trust boundary in the help text | 7 |
| §8 | `approve`/`reject`/`decide`, `--as`, `--reason` | 7 |
| §8 | `list-paused` shows menus | 8 |
| §8 | Cross-refusals | 7 |
| §9 | AC1 durable menu | 4 + 5 |
| §9 | AC2–AC5, AC11, AC14 | 5 |
| §9 | AC6 exhaustiveness | 6 |
| §9 | AC7, AC8, AC10 | 7 |
| §9 | AC9 redaction | 5 + 7 |
| §9 | AC12 e2e | 8 |
| §9 | AC13 additivity | Final gate |
