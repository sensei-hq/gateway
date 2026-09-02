# SP-6 s4 — Human Loop Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `Loop` whose stop decision is made by a person picking from an enumerated menu, once per iteration.

**Architecture:** A third `GateSpec` variant, `Human { agent, menu }`, drives a new executor arm at the already-reserved path `"{loop}/{i}/__gate__"`. The menu lives on the graph so `validate_dag` can statically reject a loop that cannot converge; the decision lives in two new journal variants so `FORMAT_VERSION` stays 1. Expiry is read **before** the decision (s2's ordering, inverting s3) and an expired undecided gate fails the loop.

**Tech Stack:** Rust, `chrono`, `serde`, `sqlx` (Postgres e2e only). Test framework is plain `cargo test`; assertions are `assert!`/`assert_eq!`.

**Spec:** `docs/superpowers/specs/2026-09-02-sp-6-s4-human-loop-gate-design.md` — acceptance criteria are referenced as `AC1`…`AC20` throughout.

---

## Preconditions (verified 2026-09-02, re-verify if time has passed)

- `RESERVED_GATE_ID = "__gate__"` exists at `crates/orchestrator-core/src/plan.rs:22` and is enforced in `feasible` at `plan.rs:147`, so an untrusted `Expand` planner cannot forge a gate node. **Confirmed.**
- `validate_dag` (`crates/orchestrator-core/src/graph.rs:384`) validates per-node-kind in numbered blocks and recurses into `Subgraph` and `LoopBody::Subgraph` bodies at block **2c** (`graph.rs:672`). A new block walking `self.nodes` therefore fires at every nesting level for free. **Confirmed.**
- `fold_journal` (`crates/orchestrator/src/executor/support.rs:67`) carries an explicit instruction at `support.rs:262`: *"this the THIRD writer of `Fold::deadlines` … when a fourth is added, update the writer lists on `Fold::deadlines` and `Fold::deadline_for`, and give it a kind-specific record of its own plus the missing-ask arm that reads it."* This slice **is** that fourth writer. Task 4 discharges it.

## Working rules for every task

- **Red first.** Write the test, run it, *see it fail for the stated reason*, then implement. A test that passes before the implementation is not a test — find out why and fix the test.
- `cargo fmt --all` before every commit. The pre-commit hook runs `fmt --check` + workspace `clippy -D warnings` and **runs no tests**, so run `cargo test --workspace` yourself before you claim green.
- Verify **real** exit codes. Never `cargo test … | tail` — the pipe's status is not the command's.
- `$DATABASE_URL` points at a **remote Supabase**. Never run the DB suite against it. Task 13 spells out the throwaway-container recipe.

## File structure

| File | Responsibility | Task |
|---|---|---|
| `crates/orchestrator-core/src/graph.rs` | `LoopGateOption`, `GateSpec::Human`, the `validate_dag` menu block | 1, 2 |
| `crates/orchestrator-core/src/journal.rs` | `LoopGateAwaited` / `LoopGateDecided` variants | 3 |
| `crates/orchestrator-core/src/lib.rs` | re-export `LoopGateOption` | 1 |
| `crates/orchestrator/src/executor/mod.rs` | `Fold` fields + accessors + writer-list docs | 4 |
| `crates/orchestrator/src/executor/support.rs` | `fold_journal` arms | 4 |
| `crates/orchestrator/src/executor/agent.rs` | extract the shared question seam | 5 |
| `crates/orchestrator/src/executor/human.rs` | `run_human_loop_gate` — the whole arm | 6, 8, 9, 10 |
| `crates/orchestrator/src/executor/fanout.rs` | the third arm in the gate match | 7 |
| `crates/orchestrator/src/executor/tests.rs` | executor tests | 6–11 |
| `crates/torii/src/cmd/gate.rs` | `decide` learns the second event pair | 12 |
| `crates/torii/src/cmd/run.rs` | cross-refusals, `list-paused` rendering | 12 |
| `crates/torii/tests/e2e_pg.rs` | cross-process e2e | 13 |

---

### Task 1: The graph types

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs:213-223` (the `GateSpec` enum)
- Modify: `crates/orchestrator-core/src/lib.rs` (re-export)
- Test: `crates/orchestrator-core/src/graph.rs` (the existing inline `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Add to the inline test module in `graph.rs`:

```rust
/// AC1 — the new variant round-trips through serde, and `stops` is what decides.
#[test]
fn a_human_gate_spec_round_trips_through_serde() {
    let gate = GateSpec::Human {
        agent: crate::registry::AgentRef("reviewer".into()),
        menu: vec![
            LoopGateOption { name: "keep-going".into(), stops: false },
            LoopGateOption { name: "good-enough".into(), stops: true },
        ],
    };
    let json = serde_json::to_string(&gate).expect("serialises");
    let back: GateSpec = serde_json::from_str(&json).expect("deserialises");
    match back {
        GateSpec::Human { agent, menu } => {
            assert_eq!(agent.0, "reviewer");
            assert_eq!(menu.len(), 2);
            assert!(!menu[0].stops, "keep-going must not stop the loop");
            assert!(menu[1].stops, "good-enough must stop the loop");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

/// AC1 — additivity: a graph using no `Human` gate serialises exactly as it does
/// today. Guards against someone adding `#[serde(tag = …)]` or reordering variants
/// and silently changing every existing `scheduled_runs.graph` row.
#[test]
fn an_existing_pure_gate_serialises_unchanged_by_the_new_variant() {
    let gate = GateSpec::Pure(LoopGate::TextContains("DONE".into()));
    assert_eq!(
        serde_json::to_string(&gate).expect("serialises"),
        r#"{"Pure":{"TextContains":"DONE"}}"#
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator-core a_human_gate_spec_round_trips_through_serde`

Expected: **compile error** — `cannot find struct `LoopGateOption`` and `no variant named `Human` found for enum `GateSpec``.

- [ ] **Step 3: Add the types**

In `graph.rs`, replace the `GateSpec` enum (currently at `:216-223`) with:

```rust
/// A `Loop`'s stop decision (SP-3 s5, extended SP-6 s4). `Pure` = the SP-1 pure predicate
/// (no journaling); `Agent` = a gate-agent over the iteration output, then a pure
/// `stop_when` over the agent's answer (the agent turn is journaled ⇒ resume replays it);
/// `Human` = a PERSON picks from an enumerated menu, once per iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GateSpec {
    Pure(LoopGate),
    Agent {
        agent: crate::registry::AgentRef,
        stop_when: LoopGate,
    },
    /// SP-6 s4. The `AgentRef` supplies the QUESTION (its `system_prompt` and activated
    /// skills) and the SLA (its `backed_by: human { timeout }`); the `menu` supplies the
    /// DECISION and lives on the graph, not the registry, so `validate_dag` can reject a
    /// menu that cannot converge — see the block in `validate_dag`.
    ///
    /// There is deliberately no `stop_when` here. Under a human backing a pure predicate
    /// would be either inert or applied to a magic option-name vocabulary, where
    /// `TextContains("halt")` against a menu emitting `"stop"` silently yields a loop that
    /// runs to `max_iters`. `LoopGateOption::stops` says the thing directly.
    Human {
        agent: crate::registry::AgentRef,
        menu: Vec<LoopGateOption>,
    },
}

/// One choice a [`GateSpec::Human`] offers, and what picking it does to the LOOP.
///
/// Deliberately NOT [`GateOption`]/[`GateOutcome`], whose `{Complete, Fail}` cannot
/// express "continue" — the one decision this variant exists for. Reinterpreting
/// `Complete` as "stop the loop" would put two meanings in a two-variant enum depending
/// on which node read it. The existing warning on [`GateOption`] — that the HITL and
/// loop-stop senses of "gate" are unrelated — is why these stay apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoopGateOption {
    /// What the operator types: `torii run gate decide … --option <name>`.
    pub name: String,
    /// `true` converges the loop; `false` runs another iteration (subject to `max_iters`).
    pub stops: bool,
}
```

- [ ] **Step 4: Re-export**

In `crates/orchestrator-core/src/lib.rs`, find the existing `pub use graph::{…}` list and add `LoopGateOption` to it, keeping the list alphabetically ordered as it already is.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator-core gate_spec`

Expected: **2 passed, 0 failed.**

- [ ] **Step 6: Confirm nothing else broke**

Run: `cargo build --workspace`

Expected: **exit 0.** Adding an enum variant makes every non-exhaustive `match` on `GateSpec` a compile error — there is exactly one, in `crates/orchestrator/src/executor/fanout.rs` at the gate match. If it errors, add a temporary arm:

```rust
GateSpec::Human { .. } => unreachable!("SP-6 s4: wired in Task 7"),
```

and delete it in Task 7. Do **not** leave it past Task 7.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/lib.rs crates/orchestrator/src/executor/fanout.rs
git commit -m "feat(core): GateSpec::Human and LoopGateOption

The third gate variant. No stop_when: under a human backing a pure
predicate is either inert or applied to a magic option-name vocabulary,
where TextContains(\"halt\") against a menu emitting \"stop\" silently
yields a loop that runs to max_iters. LoopGateOption.stops says it
directly.

Not GateOption/GateOutcome, whose {Complete, Fail} cannot express
\"continue\" — the one decision this variant exists for."
```

---

### Task 2: `validate_dag` rejects a menu that cannot converge

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs` — a new block after `2b-ter` (the `HumanGate` menu block, ends `:575`)
- Test: `crates/orchestrator-core/src/graph.rs` inline tests

This is the payoff for putting the menu on the graph. Block `2b-ter` is the direct precedent; follow its shape and its message style.

- [ ] **Step 1: Write the failing tests**

```rust
/// A `Loop` gate whose menu is valid.
fn human_gated_loop(menu: Vec<LoopGateOption>) -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId("lp".into()),
            kind: NodeKind::Loop {
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: GateSpec::Human {
                    agent: crate::registry::AgentRef("reviewer".into()),
                    menu,
                },
                max_iters: 3,
            },
            deps: vec![],
        }],
    }
}

/// AC2 — a menu with no stopping option is a loop that provably cannot converge; it
/// runs to `max_iters` however the human answers. That is a malformed graph, not a
/// policy, and it is the whole reason the menu lives on the graph rather than the
/// registry: only here can it be caught statically.
#[test]
fn validate_dag_rejects_a_human_loop_gate_with_no_stopping_option() {
    let g = human_gated_loop(vec![
        LoopGateOption { name: "again".into(), stops: false },
        LoopGateOption { name: "more".into(), stops: false },
    ]);
    let err = g.validate_dag().expect_err("must reject");
    let msg = format!("{err}");
    assert!(msg.contains("no stopping option"), "must name the defect: {msg}");
    assert!(msg.contains("lp"), "must name the node: {msg}");
}

/// AC2 — an empty menu leaves the human nothing to pick.
#[test]
fn validate_dag_rejects_a_human_loop_gate_with_an_empty_menu() {
    let err = human_gated_loop(vec![]).validate_dag().expect_err("must reject");
    assert!(format!("{err}").contains("no options"), "{err}");
}

/// AC2 — `--option x` would be ambiguous.
#[test]
fn validate_dag_rejects_a_duplicate_option_name_in_a_human_loop_gate() {
    let g = human_gated_loop(vec![
        LoopGateOption { name: "x".into(), stops: false },
        LoopGateOption { name: "x".into(), stops: true },
    ]);
    let err = g.validate_dag().expect_err("must reject");
    assert!(format!("{err}").contains("duplicate"), "{err}");
}

/// AC2 — an operator could not type it.
#[test]
fn validate_dag_rejects_an_empty_option_name_in_a_human_loop_gate() {
    let g = human_gated_loop(vec![
        LoopGateOption { name: String::new(), stops: true },
    ]);
    let err = g.validate_dag().expect_err("must reject");
    assert!(format!("{err}").contains("empty name"), "{err}");
}

/// AC2 — the valid shape is accepted. A menu with ONLY stopping options is legal:
/// "approve once, then stop" is degenerate but legitimate, and rejecting it would be
/// policy rather than structure.
#[test]
fn validate_dag_accepts_a_well_formed_human_loop_gate() {
    human_gated_loop(vec![
        LoopGateOption { name: "again".into(), stops: false },
        LoopGateOption { name: "done".into(), stops: true },
    ])
    .validate_dag()
    .expect("a menu with a stopping option is valid");

    human_gated_loop(vec![LoopGateOption { name: "done".into(), stops: true }])
        .validate_dag()
        .expect("an all-stopping menu is degenerate but legal");
}

/// AC2 — the rule fires at DEPTH. Block 2c recurses into a `Subgraph` body, so a bad
/// gate one level down must be rejected too. Without this test the block could be
/// written to walk only the top level and every nested loop gate would escape it.
#[test]
fn validate_dag_rejects_a_bad_human_loop_gate_nested_in_a_subgraph() {
    let inner = human_gated_loop(vec![
        LoopGateOption { name: "again".into(), stops: false },
    ]);
    let outer = Graph {
        nodes: vec![Node {
            id: NodeId("sub".into()),
            kind: NodeKind::Subgraph { graph: Box::new(inner) },
            deps: vec![],
        }],
    };
    let err = outer.validate_dag().expect_err("must reject at depth");
    assert!(format!("{err}").contains("no stopping option"), "{err}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator-core human_loop_gate`

Expected: **6 failures**, each `must reject: called `Result::unwrap_err()` on an `Ok` value` (except the accept test, which passes trivially). That the accept test passes now is expected and fine — it is a regression guard, not a red test.

- [ ] **Step 3: Add the validation block**

In `validate_dag`, immediately after block `2b-ter` (the `HumanGate` block, which ends with its `timeout` bounds check) and before `2b-quater`:

```rust
        // 2b-ter-bis. SP-6 s4: a `GateSpec::Human`'s menu must be usable, and must offer
        // a way to STOP. Same principle as `max_iters == 0` and 2b-ter above: reject the
        // degenerate node loudly here rather than let it produce a baffling runtime state.
        //
        // The stopping-option rule is the sharper of the two. A menu with no `stops: true`
        // option is a loop that provably cannot converge however the human answers — it
        // runs to `max_iters` and reports a non-converged result, having asked a person
        // `max_iters` times to no purpose. That is a malformed graph, and catching it is
        // the entire reason s4 puts the menu on the GRAPH rather than on the
        // `AgentDefinition`: a registry menu is invisible here, exactly as s3's §5.5
        // records for the human backing itself.
        //
        // The converse is NOT checked. A menu whose every option stops is degenerate
        // ("approve once, then stop") but legitimate, and rejecting it would be policy
        // rather than structure. Contrast 2b-ter, which DOES require a `Complete` option:
        // there, every-option-Fails is a guaranteed dead end; here, every-option-stops
        // still completes the loop normally.
        //
        // No timeout bounds are checked here — unlike 2b-ter — because a `GateSpec::Human`
        // carries no timeout. Its SLA is the ROLE's `backed_by: human { timeout }`, which
        // is a registry fact bounded where the registry is parsed (`parse_fm_duration`),
        // not a graph one.
        for node in &self.nodes {
            let NodeKind::Loop { gate: GateSpec::Human { menu, .. }, .. } = &node.kind else {
                continue;
            };
            if menu.is_empty() {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "loop node {:?} has a human gate with no options; it must offer at \
                     least one option",
                    node.id
                )));
            }
            if !menu.iter().any(|o| o.stops) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "loop node {:?} has a human gate with no stopping option, so the loop \
                     can never converge however the human answers — it would run to \
                     max_iters and ask a person that many times to no purpose; at least \
                     one option with `stops: true` is required",
                    node.id
                )));
            }
            let mut seen = HashSet::new();
            for o in menu {
                if o.name.is_empty() {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "loop node {:?} has a human gate option with an empty name; an \
                         operator could not type it",
                        node.id
                    )));
                }
                if !seen.insert(o.name.as_str()) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "loop node {:?} has a duplicate human gate option name {:?}; \
                         `--option {}` would be ambiguous",
                        node.id, o.name, o.name
                    )));
                }
            }
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator-core human_loop_gate`

Expected: **6 passed, 0 failed.**

- [ ] **Step 5: Mutation-check the nesting test**

The nesting test is the one most likely to be vacuous. Prove it is not: temporarily change the new block's `for node in &self.nodes` loop to `for node in self.nodes.iter().take(0)`, re-run `validate_dag_rejects_a_bad_human_loop_gate_nested_in_a_subgraph`, and confirm it **fails**. Revert the mutation.

Run: `cargo test -p sensei-orchestrator-core validate_dag_rejects_a_bad_human_loop_gate_nested_in_a_subgraph`

Expected under the mutation: **FAIL.** After reverting: **PASS.**

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs
git commit -m "feat(core): validate_dag rejects a human loop gate that cannot converge

A menu with no stops:true option is a loop that provably cannot converge
however the human answers — it runs to max_iters having asked a person
that many times to no purpose.

Catching this statically is the entire reason the menu lives on the GRAPH
rather than the AgentDefinition: a registry menu is invisible to
validate_dag, exactly as s3's §5.5 records for the human backing itself.

The converse is deliberately NOT checked: an all-stopping menu is
degenerate but legitimate, and rejecting it would be policy, not
structure. Block 2c's recursion carries the rule to every nesting level;
the nested test is mutation-proven, not assumed."
```

---

### Task 3: The journal variants

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs` — the `JournalEvent` enum, after the `AgentAnswered` variant
- Test: `crates/orchestrator-core/src/journal.rs` inline tests

- [ ] **Step 1: Write the failing tests**

```rust
/// AC20 — new VARIANTS are additive, so the durable format is unchanged. The existing
/// variant-count assertion in this module is the guard that a variant was added
/// deliberately; this asserts the version did not move with it.
#[test]
fn the_loop_gate_variants_do_not_move_the_format_version() {
    assert_eq!(FORMAT_VERSION, 1, "adding variants must not break the format");
}

/// The two variants round-trip, carrying everything an operator needs to see the
/// question, the menu and the deadline off the journal alone.
#[test]
fn the_loop_gate_events_round_trip() {
    let awaited = JournalEvent::LoopGateAwaited {
        node: NodeId("lp/0/__gate__".into()),
        deadline: None,
        prompt: "Continue?".into(),
        menu: vec![crate::graph::LoopGateOption { name: "done".into(), stops: true }],
    };
    let json = serde_json::to_string(&awaited).expect("serialises");
    let back: JournalEvent = serde_json::from_str(&json).expect("deserialises");
    match back {
        JournalEvent::LoopGateAwaited { node, prompt, menu, deadline } => {
            assert_eq!(node.0, "lp/0/__gate__");
            assert_eq!(prompt, "Continue?");
            assert_eq!(menu.len(), 1);
            assert!(menu[0].stops);
            assert!(deadline.is_none());
        }
        other => panic!("wrong variant: {other:?}"),
    }

    let decided = JournalEvent::LoopGateDecided {
        node: NodeId("lp/0/__gate__".into()),
        option: "done".into(),
        actor: Some("jerry".into()),
    };
    let json = serde_json::to_string(&decided).expect("serialises");
    let back: JournalEvent = serde_json::from_str(&json).expect("deserialises");
    assert!(matches!(back, JournalEvent::LoopGateDecided { .. }));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator-core loop_gate_events`

Expected: **compile error** — `no variant named `LoopGateAwaited``.

- [ ] **Step 3: Add the variants**

In `journal.rs`, after the `AgentAnswered` variant:

```rust
    /// SP-6 s4: a `GateSpec::Human` loop gate has begun asking, carrying the QUESTION and
    /// the MENU it published.
    ///
    /// The menu is journaled for s2's reason, which transfers exactly: a graph can be
    /// edited between the ask and the decision — a `scheduled_runs.graph` row, a
    /// resubmitted `run submit`, or a runtime `Expand` subgraph — and an operator's answer
    /// must keep meaning what it meant when they were asked. Reading the graph's menu at
    /// decision time would let an author flip an option's `stops` after a human picked it
    /// and silently invert their decision.
    ///
    /// The prompt is journaled for s3's reason: an operator must be able to read the
    /// question off the journal alone, and `torii`'s read path has no `Registry` and no
    /// blackboard with which to recompose it.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited`/`GateAwaited`/
    /// `AgentAwaited` do — overwriting the deadline is the never-expires bug.
    LoopGateAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: String,
        menu: Vec<crate::graph::LoopGateOption>,
    },
    /// SP-6 s4: a human picked one of a loop gate's options.
    ///
    /// A loop gate is answerable ONLY by this event, never by `SignalReceived`,
    /// `GateDecided` or `AgentAnswered` — each of the other three would bypass the
    /// menu match, and `GateDecided` would additionally carry a `GateOutcome` this kind
    /// cannot interpret.
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION: whatever string the caller supplied. It
    /// is `Option` here, unlike `GateDecided`'s required `actor`, because a loop gate can
    /// legitimately be decided by an automated operator on a schedule; the CLI still
    /// defaults it.
    LoopGateDecided {
        node: NodeId,
        option: String,
        actor: Option<String>,
    },
```

- [ ] **Step 4: Update the variant-count assertion**

`journal.rs:875` asserts `variants.len() > 10 && variants.contains(&"GateAwaited")`. Extend it to also assert `variants.contains(&"LoopGateAwaited")`, so a future refactor that drops the variant is caught.

- [ ] **Step 5: Run to verify passing**

Run: `cargo test -p sensei-orchestrator-core loop_gate`

Expected: **all passed.** Then `cargo build --workspace` — expect errors in any exhaustive `match` on `JournalEvent`. `fold_journal` has a `_` catch-all, so it will compile; Task 4 replaces that with explicit arms, which is the point.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs
git commit -m "feat(core): LoopGateAwaited / LoopGateDecided

New VARIANTS, so FORMAT_VERSION stays 1 — the additivity s3 proved with
AgentAwaited/AgentAnswered.

The menu is journaled for s2's reason, which transfers exactly: a graph
can be edited between the ask and the decision, and an operator's answer
must keep meaning what it meant. Reading the graph's menu at decision time
would let an author flip an option's stops after a human picked it and
silently invert their decision.

Not GateDecided: it carries a GateOutcome this kind cannot interpret."
```

---

### Task 4: The fold

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` — two `Fold` fields, two accessors, and the writer-list docs on `Fold::deadlines` and `Fold::deadline_for`
- Modify: `crates/orchestrator/src/executor/support.rs:230-292` — two `fold_journal` arms
- Test: `crates/orchestrator/src/executor/support.rs` inline tests

`support.rs:262` explicitly instructs that a fourth `Fold::deadlines` writer must update those doc lists. **This step is not optional bookkeeping** — three existing readers reason from that enumeration.

- [ ] **Step 1: Write the failing tests**

In `support.rs`'s inline test module:

```rust
/// The menu is FIRST-wins: a second ask must not retroactively change what a human's
/// answer meant. The decision is LAST-wins: an operator may correct it before resume.
#[test]
fn the_loop_gate_fold_is_first_wins_for_the_menu_and_last_wins_for_the_decision() {
    let node = NodeId("lp/0/__gate__".into());
    let opt = |name: &str, stops: bool| orchestrator_core::LoopGateOption {
        name: name.into(),
        stops,
    };
    let events = vec![
        (Seq(1), JournalEvent::LoopGateAwaited {
            node: node.clone(),
            deadline: None,
            prompt: "first question".into(),
            menu: vec![opt("done", true)],
        }),
        (Seq(2), JournalEvent::LoopGateAwaited {
            node: node.clone(),
            deadline: None,
            prompt: "second question".into(),
            menu: vec![opt("done", false)],
        }),
        (Seq(3), JournalEvent::LoopGateDecided {
            node: node.clone(),
            option: "done".into(),
            actor: Some("a".into()),
        }),
        (Seq(4), JournalEvent::LoopGateDecided {
            node: node.clone(),
            option: "done".into(),
            actor: Some("b".into()),
        }),
    ];
    let fold = fold_journal(&events);

    let menu = fold.loop_gate_menu_for(&node).expect("menu folded");
    assert!(menu[0].stops, "FIRST menu wins: the second ask must not flip `stops`");
    assert_eq!(
        fold.loop_gate_prompt_for(&node).expect("prompt folded"),
        "first question",
        "FIRST prompt wins"
    );
    let decision = fold.loop_gate_decision_for(&node).expect("decision folded");
    assert_eq!(decision.actor.as_deref(), Some("b"), "LAST decision wins");
}

/// `LoopGateAwaited` is the FOURTH writer of the SHARED `deadlines` map, so
/// "has this node begun asking?" still has one answer for every waiting kind. The
/// `None` is folded THROUGH — dropping it is the re-ask-every-drive bug s1 shipped.
#[test]
fn a_deadline_less_loop_gate_records_that_it_began_asking() {
    let node = NodeId("lp/0/__gate__".into());
    let fold = fold_journal(&[(Seq(1), JournalEvent::LoopGateAwaited {
        node: node.clone(),
        deadline: None,
        prompt: "q".into(),
        menu: vec![orchestrator_core::LoopGateOption { name: "done".into(), stops: true }],
    })]);
    assert_eq!(
        fold.deadline_for(&node),
        Some(None),
        "the key must be PRESENT with a None value: present = began asking, \
         None = no deadline"
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator loop_gate_fold`

Expected: **compile error** — `no method named `loop_gate_menu_for``.

- [ ] **Step 3: Add the `Fold` fields and accessors**

In `mod.rs`, beside `agent_prompts`:

```rust
    /// SP-6 s4: each loop gate's decision, from `LoopGateDecided`. LAST wins, like
    /// `signals`/`gate_decisions`/`agent_answers` and for the same reason: an operator
    /// must be able to correct a mistaken decision before the run resumes.
    loop_gate_decisions: HashMap<NodeId, LoopGateDecision>,
    /// SP-6 s4: the MENU and QUESTION each loop gate published when it began asking, from
    /// `LoopGateAwaited`. FIRST wins — the human was shown THIS menu and asked THIS
    /// question, and a later ask must not retroactively change what their answer meant.
    ///
    /// Carried as one map rather than two because both come from the same event and are
    /// read together by the same arm; splitting them would allow a state where one is
    /// present and the other is not, which no writer can produce.
    loop_gate_asks: HashMap<NodeId, LoopGateAsk>,
```

and the two records, beside `AgentAnswer`/`GateDecision`:

```rust
/// SP-6 s4: a folded `LoopGateDecided`.
pub(super) struct LoopGateDecision {
    pub(super) option: String,
    /// ATTRIBUTION, NOT AUTHENTICATION — see `JournalEvent::LoopGateDecided`.
    pub(super) actor: Option<String>,
}

/// SP-6 s4: a folded `LoopGateAwaited` — what the human was shown.
pub(super) struct LoopGateAsk {
    pub(super) prompt: String,
    pub(super) menu: Vec<orchestrator_core::LoopGateOption>,
}
```

and the three accessors, beside `menu_for`/`prompt_for`:

```rust
    /// The menu a loop gate published, or `None` if it never began asking.
    pub(super) fn loop_gate_menu_for(
        &self,
        node: &NodeId,
    ) -> Option<&[orchestrator_core::LoopGateOption]> {
        self.loop_gate_asks.get(node).map(|a| a.menu.as_slice())
    }

    /// The question a loop gate published. Answers "did the LOOP GATE kind begin here?"
    /// — narrower than [`Fold::deadline_for`], which all FOUR waiting kinds write.
    pub(super) fn loop_gate_prompt_for(&self, node: &NodeId) -> Option<&str> {
        self.loop_gate_asks.get(node).map(|a| a.prompt.as_str())
    }

    /// The decision recorded for a loop gate, if any.
    pub(super) fn loop_gate_decision_for(&self, node: &NodeId) -> Option<&LoopGateDecision> {
        self.loop_gate_decisions.get(node)
    }
```

- [ ] **Step 4: Add the `fold_journal` arms**

In `support.rs`, after the `AgentAnswered` arm at `:284`:

```rust
            // SP-6 s4: the ask. EXPLICIT, never folded by a catch-all.
            //
            // FIRST wins for the deadline, the prompt AND the menu (`entry().or_insert`).
            // This is the FOURTH writer of the SHARED `Fold::deadlines` map, after
            // `SignalAwaited`, `GateAwaited` and `AgentAwaited` — the writer lists on
            // `Fold::deadlines` and `Fold::deadline_for` name it, and the missing-ask arm
            // that reads its kind-specific record lives in `run_human_loop_gate`.
            //
            // `deadline` is folded THROUGH, `None` included. A role with
            // `backed_by: human { timeout: None }` gating a loop is a real configuration,
            // and dropping the `None` would make it re-journal `LoopGateAwaited` on every
            // drive — the bug s1 shipped on the `SignalAwaited` arm.
            JournalEvent::LoopGateAwaited {
                node,
                deadline,
                prompt,
                menu,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.loop_gate_asks.entry(node.clone()).or_insert(LoopGateAsk {
                    prompt: prompt.clone(),
                    menu: menu.clone(),
                });
            }
            // SP-6 s4: the decision. LAST wins (`insert` overwrites).
            JournalEvent::LoopGateDecided { node, option, actor } => {
                fold.loop_gate_decisions.insert(
                    node.clone(),
                    LoopGateDecision {
                        option: option.clone(),
                        actor: actor.clone(),
                    },
                );
            }
```

- [ ] **Step 5: Discharge the `support.rs:262` instruction**

Update three doc comments to name the fourth writer:

1. `mod.rs` `Fold::deadlines` — the sentence *"folded from `SignalAwaited` and — since SP-6 s2 and s3 — from `GateAwaited` and `AgentAwaited` too, so that … has ONE answer for all THREE waiting kinds"* becomes **four** kinds, adding `LoopGateAwaited`, and the reader list gains `run_human_loop_gate`'s missing-ask arm paired with `Fold::loop_gate_asks`.
2. `mod.rs` `Fold::deadline_for` — its `None` bullet lists the three writers; add `LoopGateAwaited` and change "the three writers" to "the four writers".
3. `support.rs:262` — the "when a fourth is added" instruction becomes "when a **fifth** is added", so the next person gets the same prompt.

- [ ] **Step 6: Run to verify passing**

Run: `cargo test -p sensei-orchestrator loop_gate_fold` then `cargo test -p sensei-orchestrator a_deadline_less_loop_gate_records_that_it_began_asking`

Expected: **2 passed, 0 failed.**

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs
git commit -m "feat(orchestrator): fold the loop-gate events

The FOURTH writer of the shared Fold::deadlines map. support.rs:262 left
a standing instruction for exactly this moment — update the writer lists
on Fold::deadlines and Fold::deadline_for, and give the kind its own
record plus the missing-ask arm that reads it. All three are discharged
here, and the instruction now says 'a fifth'.

Menu and prompt FIRST-wins, decision LAST-wins. The deadline None is
folded THROUGH: a human-gated loop with timeout: None is a real
configuration, and dropping it makes the gate re-ask on every drive —
the bug s1 shipped on the SignalAwaited arm."
```

---

### Task 5: Extract the shared question seam

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs:82-107` (the head of `drive_agent`)
- Test: `crates/orchestrator/src/executor/tests.rs`

Behaviour-preserving extraction. The existing s3 tests are the regression guard; the new test pins the property the extraction exists to protect.

- [ ] **Step 1: Write the failing test**

```rust
/// The human loop gate's question is built by the MODEL path's own prompt assembly, so
/// the two cannot drift on what "the agent's prompt" means. s3 put its human branch
/// inside `drive_agent` for exactly this reason; s4 does not go through `drive_agent`,
/// so the property has to be preserved by a shared function instead.
///
/// Asserts the seam EXISTS and returns the human backing's timeout — without it, s4
/// would need a second prompt builder, which is the drift this guards.
#[tokio::test]
async fn the_human_question_seam_composes_the_same_prompt_the_model_path_would() {
    let exec = executor_with_human_reviewer(); // helper from the s3 tests
    let (question, timeout) = exec
        .human_question_for(
            &AgentRef("reviewer".into()),
            &serde_json::json!("the Acme MSA"),
            &[],
        )
        .expect("a human-backed role composes a question");

    assert!(
        question.text().contains("the Acme MSA"),
        "the node input reaches the question: {}",
        question.text()
    );
    assert!(
        timeout.is_some(),
        "the role's backed_by: human timeout is returned, so the caller need not \
         re-read the registry"
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator the_human_question_seam_composes_the_same_prompt`

Expected: **compile error** — `no method named `human_question_for``.

- [ ] **Step 3: Extract**

Add to `agent.rs`:

```rust
    /// Resolve a human-backed `AgentRef` into the QUESTION to ask and the SLA to ask it
    /// under — the seam `drive_agent`'s human branch and `run_human_loop_gate` share.
    ///
    /// It exists so s4 needs no second prompt builder. s3's central property is that a
    /// human's question is composed by the MODEL path's own `assemble_prompt_parts`, so
    /// the two cannot drift on what "the agent's prompt" means; s4 does not route through
    /// `drive_agent` (it has no ReAct loop, no turns and no `stop_when`, and threading a
    /// menu through `drive_agent` would put a parameter there every model caller must pass
    /// as `None`). Sharing this function is what keeps the property without the coupling.
    ///
    /// Fails loudly on a MODEL-backed role. Silence would let an author believe a person
    /// is in the loop while the run quietly decides for itself — the mirror of the refusal
    /// `drive_agent` gives a human role at an illegal position.
    pub(super) fn human_question_for(
        &self,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        context: &[(ContextKey, serde_json::Value)],
    ) -> Result<(HumanQuestion, Option<chrono::Duration>), OrchestratorError> {
        let agent: &AgentDefinition = self
            .registry
            .agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let orchestrator_core::AgentBacking::Human { timeout } = agent.backed_by else {
            return Err(OrchestratorError::InvalidGraph(format!(
                "agent {:?} is model-backed but is named where a human-backed role is \
                 required; set `backed_by: human` in its frontmatter, or use a gate kind \
                 that takes a model",
                agent_ref.0
            )));
        };
        let query = render_input(input);
        let parts = assemble_prompt_parts(&self.registry, agent, context, &query)?;
        Ok((
            HumanQuestion::compose(&parts.authored, &parts.context, &query, |t| {
                self.redact_text(t)
            }),
            timeout,
        ))
    }
```

Then rewrite `drive_agent`'s human branch to call it. **Take care:** `drive_agent` looks the agent up *before* it knows the backing, and needs the definition for the model path too. Keep the existing lookup, and in the `AgentBacking::Human` branch call `human_question_for` rather than recomposing. If that double lookup offends, leave it — it is a `HashMap` hit and clarity wins.

Add `pub(super) fn text(&self) -> &str` to `HumanQuestion` if it has no accessor; the test needs one.

- [ ] **Step 4: Run to verify passing, and that s3 did not regress**

Run: `cargo test -p sensei-orchestrator human_agent`
Run: `cargo test -p sensei-orchestrator the_human_question_seam_composes_the_same_prompt`

Expected: **all s3 human-agent tests still pass**, plus the new one. If any s3 test reddens, the extraction changed behaviour — fix the extraction, not the test.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/human.rs crates/orchestrator/src/executor/tests.rs
git commit -m "refactor(orchestrator): extract the human-question seam

s3's central property is that a human's question is composed by the MODEL
path's own assemble_prompt_parts, so the two cannot drift on what 'the
agent's prompt' means. s4 does not route through drive_agent — it has no
ReAct loop, no turns and no stop_when, and threading a menu through
drive_agent would put a parameter there every model caller passes as None.

Sharing this function keeps the property without the coupling. Behaviour
preserving; the s3 suite is the regression guard."
```

---

### Task 6: `run_human_loop_gate` — ask, and honour a decision

**Files:**
- Modify: `crates/orchestrator/src/executor/human.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

The core arm. Structure follows `run_human_gate` (s2), **not** `run_human_agent` (s3) — the expiry ordering is the difference and Task 8 pins it.

- [ ] **Step 1: Write the failing tests (AC3, AC4, AC5, AC6)**

```rust
/// A `Loop` gated by a human, with `max_iters` iterations of a `ModelCall` body.
fn human_gated_loop_graph(max_iters: u32) -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId("lp".into()),
            kind: NodeKind::Loop {
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "draft it" }),
                gate: GateSpec::Human {
                    agent: AgentRef("reviewer".into()),
                    menu: vec![
                        LoopGateOption { name: "revise".into(), stops: false },
                        LoopGateOption { name: "ship".into(), stops: true },
                    ],
                },
                max_iters,
            },
            deps: vec![],
        }],
    }
}

/// AC3 — after iteration 0 the gate journals ONE `LoopGateAwaited` at the reserved
/// path, carrying the question and the menu, and the run pauses.
#[tokio::test]
async fn a_human_loop_gate_asks_once_per_iteration_and_pauses() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("drives");
    assert!(out.paused.is_some(), "the run pauses on the human gate: {out:?}");

    let events = journal.load(run_id()).await.expect("journal loads");
    let asks: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::LoopGateAwaited { node, menu, prompt, .. } => {
                Some((node.clone(), menu.clone(), prompt.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(asks.len(), 1, "exactly one question so far: {asks:?}");
    assert_eq!(asks[0].0 .0, "lp/0/__gate__", "at the reserved gate path");
    assert_eq!(asks[0].1.len(), 2, "the menu is journaled");
    assert!(!asks[0].2.is_empty(), "the question is journaled");
}

/// AC4 — a `stops: true` decision converges the loop.
#[tokio::test]
async fn a_stopping_decision_converges_the_loop() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("pauses");

    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "ship".into(),
            actor: Some("jerry".into()),
        })
        .await
        .expect("decision lands");

    let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("resumes");
    assert!(out.failed.is_none(), "the loop completes: {out:?}");
    assert!(out.paused.is_none(), "and does not pause again: {out:?}");
    assert!(out.completed.contains(&NodeId("lp".into())), "the Loop completed");
}

/// AC5 + AC3 — a `stops: false` decision runs ANOTHER iteration, which asks its OWN
/// question at its own path. This is the feature: per-iteration re-asking, authored
/// deliberately at a site designed for it.
#[tokio::test]
async fn a_continuing_decision_runs_another_iteration_that_asks_again() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("pauses");
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "revise".into(),
            actor: None,
        })
        .await
        .expect("decision lands");

    let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("resumes");
    assert!(out.paused.is_some(), "it pauses again on iteration 1's gate: {out:?}");

    let events = journal.load(run_id()).await.expect("loads");
    let paths: Vec<String> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::LoopGateAwaited { node, .. } => Some(node.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        paths,
        vec!["lp/0/__gate__".to_string(), "lp/1/__gate__".to_string()],
        "iteration 1 asks its OWN question at its OWN path"
    );
}

/// AC6 — `max_iters` still bounds a human who keeps choosing to continue. Without this
/// the gate would be an unbounded prompt for human attention.
#[tokio::test]
async fn max_iters_bounds_a_human_who_keeps_continuing() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    let graph = human_gated_loop_graph(2);
    for i in 0..2 {
        exec.start(run_id(), &graph).await.expect("drives");
        journal
            .append(run_id(), JournalEvent::LoopGateDecided {
                node: NodeId(format!("lp/{i}/__gate__")),
                option: "revise".into(),
                actor: None,
            })
            .await
            .expect("decision lands");
    }
    let out = exec.start(run_id(), &graph).await.expect("resumes");
    assert!(out.paused.is_none(), "capped at max_iters, not paused again: {out:?}");
    assert!(out.completed.contains(&NodeId("lp".into())), "the Loop completed at its cap");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator human_loop_gate`

Expected: **failures** — the `unreachable!` from Task 1 Step 6 panics, or the gate never asks. Either is the correct red.

- [ ] **Step 3: Implement `run_human_loop_gate`**

In `human.rs`, mirroring `run_human_gate`'s structure. Return `Result<LoopGateStep, OrchestratorError>` where:

```rust
/// What a loop gate decided, in the shape `run_loop` needs.
pub(super) enum LoopGateStep {
    /// The human decided; `stop` is the chosen option's `stops`.
    Decided { stop: bool },
    /// The gate failed — expiry, an unmatched option, a config error. `run_loop` turns
    /// this into a failed Loop.
    Failed(String),
    /// Waiting on a human; `run_loop` propagates the pause.
    Paused(PauseReason),
}
```

The body, in order:

```rust
    pub(super) async fn run_human_loop_gate(
        &self,
        run: RunId,
        node_id: &NodeId,
        agent_ref: &AgentRef,
        menu: &[orchestrator_core::LoopGateOption],
        iteration_output: &serde_json::Value,
        fold: &Fold,
    ) -> Result<LoopGateStep, OrchestratorError> {
        // 0. Already failed ⇒ stays failed, verdict READ BACK not re-derived. Same
        //    unbounded-journal-growth fix as s3: this refusal is terminal for the node,
        //    but the run it kills journals no `RunCompleted`, so every later wake would
        //    otherwise re-drive the iteration and append a fresh `NodeFailed`.
        if let Some(failed) = self.gate_precheck_by_id(node_id, fold) {
            return Ok(LoopGateStep::Failed(failed_message(failed)));
        }

        // 1. The question and the SLA, through the shared seam. A model-backed role here
        //    fails loudly (AC14).
        let (question, timeout) =
            match self.human_question_for(agent_ref, iteration_output, &[]) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(LoopGateStep::Failed(
                        self.fail_loop_gate(run, node_id, format!("loop_gate: {e}")).await?,
                    ));
                }
            };

        // 2. Wait state — ACTED ON IMMEDIATELY. This is the s2 ordering and the
        //    deliberate inversion of s3; see the doc comment and AC8.
        match self.wait_or_expire_by_id(node_id, timeout, fold) { … }
```

with these arms:

- `Err(message)` → `fail_loop_gate(run, node_id, format!("loop_gate: {message}"))`.
- `Ok(WaitState::Expired(d))` → fail, naming the deadline and **not** "no decision" (this arm has not read the fold and cannot know whether one exists — the exact wording bug s2's review caught).
- `Ok(WaitState::NotYetAsking(fresh))` → bound `question.authored_bytes` against `MAX_HUMAN_TEXT_BYTES` (loud fail, message naming the role's `system_prompt`/skills and stating that `## Context` is **not** counted), then `question.redact_and_clamp(|t| self.redact_text(t), MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES)`, append `LoopGateAwaited { node, deadline: fresh, prompt, menu: menu.to_vec() }`, return `Paused`.
- `Ok(WaitState::Waiting(d))` → read the **journaled** menu via `fold.loop_gate_menu_for(node_id)`. Absent ⇒ a kind-swapped node (the shared `deadlines` map has four writers) ⇒ fail loudly, never fall back to the graph's menu. Then read `fold.loop_gate_decision_for(node_id)`; absent ⇒ `Paused`; present ⇒ match `option` against the journaled menu; unmatched ⇒ fail loudly (Task 9); matched ⇒ `Decided { stop: o.stops }`.

Add `fail_loop_gate`, mirroring `fail_human_agent`, so this arm cannot skip the redaction chokepoint.

- [ ] **Step 4: Run to verify passing**

Run: `cargo test -p sensei-orchestrator human_loop_gate`

Expected: **4 passed, 0 failed** (these will only fully pass once Task 7 wires the arm; run Task 7 first if the gate is still `unreachable!`).

- [ ] **Step 5: Commit** (fold into Task 7's commit if the arm is not yet reachable)

---

### Task 7: Wire the arm into `run_loop`

**Files:**
- Modify: `crates/orchestrator/src/executor/fanout.rs:545-571`

- [ ] **Step 1: Replace the temporary arm**

Delete the `unreachable!` placeholder from Task 1 and add:

```rust
                // SP-6 s4: a PERSON decides. Same reserved path as the gate-agent, but no
                // agent is driven: `run_human_loop_gate` never resolves a chain, so zero
                // token spend is STRUCTURAL. That matters more here than anywhere else in
                // SP-6 — the decision being made IS whether to spend more.
                GateSpec::Human { agent, menu } => {
                    let gate_path = NodeId(format!("{path}/__gate__"));
                    match self
                        .run_human_loop_gate(run, &gate_path, agent, menu, &output, fold)
                        .await?
                    {
                        LoopGateStep::Decided { stop } => stop,
                        // A gate failure fails the Loop, exactly as the gate-AGENT arm
                        // above does — no new outcome shape (AC10).
                        LoopGateStep::Failed(m) => {
                            let msg = format!(
                                "loop {:?} human gate failed at iteration {i}: {m}",
                                loop_node.id
                            );
                            return self.fail_loop(run, &loop_node.id, msg).await;
                        }
                        LoopGateStep::Paused(reason) => {
                            return Ok(NodeExec::Paused { reason })
                        }
                    }
                }
```

- [ ] **Step 2: Run the Task 6 tests**

Run: `cargo test -p sensei-orchestrator human_loop_gate`

Expected: **4 passed, 0 failed.**

- [ ] **Step 3: Run the whole suite**

Run: `cargo test --workspace`

Expected: **0 failed, exit 0.** Check the real exit code with `echo $?` — do not pipe.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/human.rs crates/orchestrator/src/executor/fanout.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): a human decides whether the loop continues

run_human_loop_gate at the reserved {loop}/{i}/__gate__ path. Structure
follows run_human_gate (s2), not run_human_agent (s3): the wait state is
acted on immediately, so expiry is read BEFORE the decision.

No agent is driven and no chain is resolved, so zero token spend is
structural — which matters more here than anywhere else in SP-6, because
the decision being made IS whether to spend more.

A gate failure fails the Loop, exactly as the gate-agent arm already
does. No new outcome shape."
```

---

### Task 8: Expiry is read BEFORE the decision

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

The single most important test in the slice. It is the one that reddens if someone "simplifies" the arm into s3's shape.

- [ ] **Step 1: Write the failing tests (AC8, AC9, AC10)**

```rust
/// AC8 — **the ordering test.** A decision that lands AFTER the deadline does not
/// continue the loop. This inverts s3, deliberately: an agent's answer is work product
/// with nothing to self-approve, but "continue" AUTHORIZES ANOTHER ITERATION OF SPEND,
/// which is an approval in the sense s2 built its ordering for. Honouring a late
/// "continue" would sanction tokens the operator's own SLA said to stop waiting for.
///
/// This test reddens if the arm is reordered to read the decision first.
#[tokio::test]
async fn a_decision_after_the_deadline_does_not_continue_the_loop() {
    let clock = FakeClock::new();
    let (exec, journal) = executor_with_human_reviewer_at(clock.clone()); // 1h timeout
    let graph = human_gated_loop_graph(3);
    exec.start(run_id(), &graph).await.expect("pauses on the gate");

    // The SLA runs out, THEN the decision lands.
    clock.advance(chrono::Duration::hours(2));
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "revise".into(),
            actor: Some("late".into()),
        })
        .await
        .expect("decision lands late");

    let out = exec.start(run_id(), &graph).await.expect("resumes");
    assert!(out.failed.is_some(), "the loop FAILS on its deadline: {out:?}");
    let reason = format!("{:?}", out.failed);
    assert!(
        reason.contains("deadline"),
        "the failure names the deadline, not 'no decision' — this arm has not read the \
         fold and cannot know whether one exists: {reason}"
    );

    // And the loop did NOT run a second iteration off the late decision.
    let events = journal.load(run_id()).await.expect("loads");
    let asks = events
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::LoopGateAwaited { .. }))
        .count();
    assert_eq!(asks, 1, "no iteration 1 was started by the late decision");
}

/// AC9 — a FIRED expiry is terminal. A decision arriving after the failure was
/// journaled cannot resurrect the gate, and re-driving must not append a second
/// `NodeFailed` for an already-dead node.
#[tokio::test]
async fn a_fired_loop_gate_expiry_is_terminal_and_appends_no_second_failure() {
    let clock = FakeClock::new();
    let (exec, journal) = executor_with_human_reviewer_at(clock.clone());
    let graph = human_gated_loop_graph(3);
    exec.start(run_id(), &graph).await.expect("pauses");
    clock.advance(chrono::Duration::hours(2));
    exec.start(run_id(), &graph).await.expect("fails on the deadline");

    let before = count_node_failed(&journal.load(run_id()).await.unwrap());
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "ship".into(),
            actor: Some("too-late".into()),
        })
        .await
        .expect("lands");
    let out = exec.start(run_id(), &graph).await.expect("re-drives");

    assert!(out.failed.is_some(), "still failed: {out:?}");
    assert_eq!(
        count_node_failed(&journal.load(run_id()).await.unwrap()),
        before,
        "the verdict is READ BACK, not re-derived — no second NodeFailed"
    );
}
```

- [ ] **Step 2: Run to verify they fail if the ordering is wrong**

Run: `cargo test -p sensei-orchestrator a_decision_after_the_deadline`

Expected: **PASS** if Task 6 implemented the ordering correctly. **This is the one case where a green first run is acceptable** — but you must then prove the test is not vacuous.

- [ ] **Step 3: Mutation-prove the ordering test**

Temporarily move the decision read **above** the `WaitState::Expired` arm in `run_human_loop_gate` (s3's ordering). Re-run:

Run: `cargo test -p sensei-orchestrator a_decision_after_the_deadline_does_not_continue_the_loop`

Expected under the mutation: **FAIL.** Revert. Re-run: **PASS.** If it passes under the mutation the test is not guarding the ordering — fix the test before continuing.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): expiry is read BEFORE the loop-gate decision

The ordering test, mutation-proven: reordering the arm to s3's shape
reddens it.

s3 reads the answer first because an agent's answer is work product with
nothing to self-approve. That does not transfer. 'Continue' authorizes
another iteration of spend, which is an approval in the sense s2 built
its ordering for, so honouring a late one sanctions tokens the operator's
own SLA said to stop waiting for.

Also pins that the failure names the DEADLINE, never 'no decision' — the
arm has not read the fold and cannot know whether one exists."
```

---

### Task 9: The three loud refusals

**Files:**
- Modify: `crates/orchestrator/src/executor/human.rs` (unmatched option), `crates/orchestrator/src/executor/agent.rs` (the `GateSpec::Agent` message)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests (AC13, AC14, AC14b)**

```rust
/// AC14b — a decision naming an option ABSENT from the journaled menu fails loudly. It
/// neither continues nor stops: defaulting either way would be a decision no human made
/// — to stop, or to spend more. Reachable only from a journal `torii` did not write,
/// which is exactly why it must fail rather than guess.
#[tokio::test]
async fn a_decision_naming_an_unknown_option_fails_the_loop_gate() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    let graph = human_gated_loop_graph(3);
    exec.start(run_id(), &graph).await.expect("pauses");
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "sideways".into(),
            actor: None,
        })
        .await
        .expect("lands");

    let out = exec.start(run_id(), &graph).await.expect("resumes");
    assert!(out.failed.is_some(), "must fail, not guess: {out:?}");
    let msg = format!("{:?}", out.failed);
    assert!(msg.contains("sideways"), "names the bad option: {msg}");
    assert!(msg.contains("revise") && msg.contains("ship"), "recites the menu: {msg}");
}

/// AC14 — a MODEL-backed role named in `GateSpec::Human` fails loudly. Silence would
/// let an author believe a person is in the loop while the run quietly decides for
/// itself.
#[tokio::test]
async fn a_model_backed_role_in_a_human_loop_gate_fails_loudly() {
    let exec = executor_with_model_reviewer(); // `reviewer` is backed_by: model
    let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("drives");
    assert!(out.failed.is_some(), "must refuse: {out:?}");
    assert!(
        format!("{:?}", out.failed).contains("backed_by: human"),
        "the message must say how to fix it: {:?}",
        out.failed
    );
}

/// AC13 — a HUMAN-backed role in `GateSpec::Agent` STILL refuses, and now names the
/// variant that would work. This slice adds no new path into `drive_agent`, so the
/// Subgraph-wrapper bypass the s3 review closed stays shut.
#[tokio::test]
async fn a_human_role_in_gate_spec_agent_still_refuses_and_names_gate_spec_human() {
    let exec = executor_with_human_reviewer();
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("G".into()),
            kind: NodeKind::Loop {
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: GateSpec::Agent {
                    agent: AgentRef("reviewer".into()),
                    stop_when: LoopGate::TextContains("NEVER".into()),
                },
                max_iters: 1,
            },
            deps: vec![],
        }],
    };
    let out = exec.start(run_id(), &graph).await.expect("drives");
    assert!(out.failed.is_some(), "still refused: {out:?}");
    assert!(
        format!("{:?}", out.failed).contains("GateSpec::Human"),
        "the refusal names the variant that WOULD work: {:?}",
        out.failed
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator -- unknown_option model_backed_role_in_a_human gate_spec_agent_still_refuses`

Expected: **3 failures.**

- [ ] **Step 3: Implement**

1. In `run_human_loop_gate`'s decided arm, the unmatched branch:

```rust
        let Some(chosen) = published.iter().find(|o| o.name == decision.option) else {
            return Ok(LoopGateStep::Failed(
                self.fail_loop_gate(
                    run,
                    node_id,
                    format!(
                        "loop_gate: node {} was decided with option {:?}, which is not in \
                         the menu it published: {}. The gate neither continues nor stops — \
                         defaulting either way would be a decision no human made.",
                        node_id.0,
                        decision.option,
                        published.iter().map(|o| o.name.as_str()).collect::<Vec<_>>().join(", ")
                    ),
                )
                .await?,
            ));
        };
```

2. `human_question_for` already fails loudly on a model backing (Task 5).

3. In `agent.rs`'s `!top_level` refusal, extend the message with: `"(a human-backed role gating a Loop belongs in GateSpec::Human, which takes a menu; GateSpec::Agent drives a model and applies a pure stop_when to its answer)"`.

- [ ] **Step 4: Run to verify passing, and that the site table still holds**

Run: `cargo test -p sensei-orchestrator -- unknown_option model_backed_role_in_a_human gate_spec_agent_still_refuses`
Run: `cargo test -p sensei-orchestrator non_top_level`

Expected: **all pass.** The `non_top_level_sites` table must be **unchanged** — if a row had to be deleted, a bypass was opened. Stop and re-read §5.4 of the spec.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/human.rs crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): the three loud refusals

An unmatched option fails rather than guessing: defaulting either way
would be a decision no human made — to stop, or to spend more. Reachable
only from a journal torii did not write, which is why it must fail.

A model-backed role in GateSpec::Human fails loudly; silence would let an
author believe a person is in the loop while the run decides for itself.

A human-backed role in GateSpec::Agent STILL refuses and now names
GateSpec::Human. non_top_level_sites is unchanged — this slice adds no
new path into drive_agent, so the Subgraph-wrapper bypass stays shut."
```

---

### Task 10: Bounds and redaction

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

The behaviour comes free from the shared seam; these tests prove it did not get lost.

- [ ] **Step 1: Write the tests (AC15, AC16)**

```rust
/// AC15 — the AUTHORED half fails loudly over the cap. A config error, actionable by
/// the person who wrote the role.
#[tokio::test]
async fn an_oversized_authored_prompt_fails_the_loop_gate() {
    let exec = executor_with_human_reviewer_whose_system_prompt_is("x".repeat(5000));
    let out = exec.start(run_id(), &human_gated_loop_graph(2)).await.expect("drives");
    assert!(out.failed.is_some(), "must fail: {out:?}");
    assert!(
        format!("{:?}", out.failed).contains("authored prompt"),
        "and blame the right half: {:?}",
        out.failed
    );
}

/// AC15 — the `## Context` half TRUNCATES instead. It is the ITERATION OUTPUT, i.e. run
/// data no operator can bound at config time, and a loop gate's context is a model
/// iteration's output essentially always. Charging one cap against both would kill the
/// node on ordinary data, after the iteration's tokens were already spent.
#[tokio::test]
async fn a_verbose_iteration_output_truncates_the_question_instead_of_killing_the_gate() {
    let (exec, journal) = executor_with_human_reviewer_and_verbose_body(); // ~50 KiB output
    let out = exec.start(run_id(), &human_gated_loop_graph(2)).await.expect("drives");
    assert!(out.failed.is_none(), "a verbose upstream must not kill the gate: {out:?}");
    assert!(out.paused.is_some(), "it pauses on the question: {out:?}");

    let events = journal.load(run_id()).await.expect("loads");
    let prompt = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::LoopGateAwaited { prompt, .. } => Some(prompt.clone()),
            _ => None,
        })
        .expect("a question was journaled");
    assert!(
        prompt.len() <= MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES,
        "the question is bounded: {} bytes",
        prompt.len()
    );
}

/// AC16 — the journaled prompt is REDACTED before the durable write, not only at
/// display time. This is the one place a credential in a role's system_prompt, a skill
/// body, or the iteration output would land in the clear: `torii config push` redacts
/// nothing.
#[tokio::test]
async fn the_journaled_loop_gate_question_is_redacted() {
    let secret = format!("sk-{}", "a".repeat(40)); // assembled at runtime, never a literal
    let (exec, journal) = executor_with_human_reviewer_whose_body_emits(&secret);
    exec.start(run_id(), &human_gated_loop_graph(2)).await.expect("drives");

    let events = journal.load(run_id()).await.expect("loads");
    let prompt = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::LoopGateAwaited { prompt, .. } => Some(prompt.clone()),
            _ => None,
        })
        .expect("a question was journaled");
    assert!(!prompt.contains(&secret), "the secret must not reach the journal");
    assert!(prompt.contains("[REDACTED]"), "it must be visibly redacted: {prompt}");
}
```

> **Note:** the Semgrep CWE-798 pre-commit hook blocks literal credential-shaped fixtures. Assemble the secret at runtime as shown — never inline the full string.

- [ ] **Step 2: Run, implement if needed, run again**

Run: `cargo test -p sensei-orchestrator -- oversized_authored_prompt_fails_the_loop_gate verbose_iteration_output_truncates journaled_loop_gate_question_is_redacted`

Expected: **all pass** if Task 6 used `question.authored_bytes` and `redact_and_clamp` as specified. If the redaction test fails, the arm is appending `question.text()` directly — route it through `redact_and_clamp`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): loop-gate bounds and redaction

The two-cap rule holds at this site: the authored half fails loudly, the
## Context half truncates. A loop gate's context is a model iteration's
output essentially always, so charging one cap against both would kill
the node on ordinary data after those tokens were already spent — s3's
whole-slice fix, load-bearing here.

And the journaled question is redacted before the durable write: torii
config push redacts nothing, so this is the one place a credential in a
system_prompt or skill body would land in the clear."
```

---

### Task 11: Zero spend and resume

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the tests (AC11, AC12)**

```rust
/// AC11 — the gate itself spends NOTHING. Structural, not measured: the arm never
/// resolves a chain. This matters more here than at any other human site, because the
/// decision being made IS whether to spend more — a gate that cost tokens would be
/// self-undermining.
#[tokio::test]
async fn a_human_loop_gate_spends_no_tokens() {
    let (exec, journal, gateway) = executor_with_human_reviewer_and_counting_gateway();
    exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("pauses on the gate");

    let events = journal.load(run_id()).await.expect("loads");
    let gate_effects = events
        .iter()
        .filter(|(_, e)| matches!(
            e,
            JournalEvent::EffectRecorded { node, .. } if node.0.ends_with("__gate__")
        ))
        .count();
    assert_eq!(gate_effects, 0, "the gate journals no effect");
    assert_eq!(
        gateway.calls(),
        1,
        "exactly one gateway call — iteration 0's BODY. The gate made none."
    );
}

/// AC12 — a decided gate replays from the journal: no re-ask, no gateway call, and the
/// identical decision. The pure part (`stops` → converged) is recomputed from the
/// journaled option name.
#[tokio::test]
async fn a_decided_loop_gate_replays_from_the_journal_without_re_asking() {
    let (exec, journal, gateway) = executor_with_human_reviewer_and_counting_gateway();
    let graph = human_gated_loop_graph(3);
    exec.start(run_id(), &graph).await.expect("pauses");
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "ship".into(),
            actor: None,
        })
        .await
        .expect("lands");

    let calls_before = gateway.calls();
    let out = exec.start(run_id(), &graph).await.expect("resumes");
    assert!(out.completed.contains(&NodeId("lp".into())), "converged: {out:?}");
    assert_eq!(
        gateway.calls(),
        calls_before,
        "iteration 0's body replays from its memo and the gate re-asks nothing — \
         zero re-spend"
    );

    let asks = journal
        .load(run_id())
        .await
        .unwrap()
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::LoopGateAwaited { .. }))
        .count();
    assert_eq!(asks, 1, "the question was asked ONCE across both drives");
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p sensei-orchestrator -- spends_no_tokens replays_from_the_journal_without_re_asking`

Expected: **2 passed.**

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): the loop gate spends nothing and replays clean

Zero spend is structural — the arm never resolves a chain — and it
matters more here than at any other human site, because the decision
being made IS whether to spend more.

A decided gate replays from LoopGateDecided with no re-ask and no gateway
call; the pure part is recomputed from the journaled option name."
```

---

### Task 12: The `torii` operator surface

**Files:**
- Modify: `crates/torii/src/cmd/gate.rs` — `gate_menu` and `decide` learn the second event pair
- Modify: `crates/torii/src/cmd/run.rs` — cross-refusals and `list-paused` rendering
- Test: `crates/torii/src/cmd/gate.rs` and `crates/torii/src/cmd/run.rs` inline tests

`gate_menu` (`gate.rs:716`) already reads the menu from the **journal**, which is why this extends rather than rewrites.

- [ ] **Step 1: Write the failing tests (AC17, AC18)**

```rust
/// AC17 — `run gate decide` decides a LOOP gate at its synthetic path. The node does
/// not exist in the graph; the menu comes from the journal, which is what makes this
/// work at all.
#[tokio::test]
async fn gate_decide_decides_a_loop_gate() {
    let ctx = ctx_with_journal(vec![loop_gate_awaited("lp/0/__gate__", &["revise", "ship"])]);
    let out = decide(&ctx, run_id(), "lp/0/__gate__", "ship", None).await.expect("decides");
    assert!(out.text.contains("ship"), "confirms the choice: {}", out.text);

    let appended = ctx.journal.load(run_id()).await.unwrap();
    assert!(
        appended.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::LoopGateDecided { option, .. } if option == "ship"
        )),
        "a LoopGateDecided is appended — never a GateDecided, which carries a \
         GateOutcome this kind cannot interpret"
    );
}

/// AC17 — a bad option recites the JOURNALED menu so the operator can retry.
#[tokio::test]
async fn gate_decide_recites_a_loop_gates_menu_on_a_bad_option() {
    let ctx = ctx_with_journal(vec![loop_gate_awaited("lp/0/__gate__", &["revise", "ship"])]);
    let err = decide(&ctx, run_id(), "lp/0/__gate__", "sideways", None).await.expect_err("refuses");
    let msg = format!("{err}");
    assert!(msg.contains("revise") && msg.contains("ship"), "recites the menu: {msg}");
}

/// AC17 — the cross-refusals. Each names the verb that WOULD work.
#[tokio::test]
async fn signal_and_agent_answer_refuse_a_loop_gate() {
    let ctx = ctx_with_journal(vec![loop_gate_awaited("lp/0/__gate__", &["ship"])]);

    let err = signal(&ctx, run_id(), "lp/0/__gate__", "{}").await.expect_err("refuses");
    assert!(
        format!("{err}").contains("run gate decide"),
        "names the verb that works: {err}"
    );

    let err = agent_answer(&ctx, run_id(), "lp/0/__gate__", "yes").await.expect_err("refuses");
    assert!(format!("{err}").contains("run gate decide"), "{err}");
}

/// AC18 — `list-paused` renders the question and the menu, so an operator sees what is
/// being asked without reading the graph or the registry.
#[tokio::test]
async fn list_paused_renders_a_loop_gates_question_and_menu() {
    let ctx = ctx_with_journal(vec![loop_gate_awaited("lp/0/__gate__", &["revise", "ship"])]);
    let out = list_paused(&ctx).await.expect("lists");
    assert!(out.text.contains("lp/0/__gate__"), "names the node: {}", out.text);
    assert!(out.text.contains("revise"), "shows the menu: {}", out.text);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-torii -- loop_gate`

Expected: **4 failures** — `decide` reports the node has not asked, because `gate_menu` reads only `GateAwaited`.

- [ ] **Step 3: Implement**

1. `gate_menu` returns an enum rather than `Option<Vec<GateOption>>`:

```rust
/// Which waiting kind published a menu at this node, and what it published. Two kinds
/// carry menus and they are NOT interchangeable: a `HumanGate`'s options carry a
/// `GateOutcome`, a loop gate's carry `stops`. `decide` must append the matching event.
pub(crate) enum PublishedMenu {
    Human(Vec<GateOption>),
    Loop(Vec<LoopGateOption>),
}
```

2. `decide` matches on it and appends `GateDecided` or `LoopGateDecided` accordingly. The option-matching, menu recital and cap logic is shared — factor it over the option NAMES, which both kinds have.

3. `run.rs`'s `signal` and `agent answer` gain a `LoopGateAwaited` arm in their state check, refusing with `run gate decide`.

4. `render::awaiting_section` gains a loop-gate arm showing the question and menu.

- [ ] **Step 4: Run to verify passing**

Run: `cargo test -p sensei-torii`

Expected: **0 failed.**

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/gate.rs crates/torii/src/cmd/run.rs
git commit -m "feat(torii): decide a loop gate

gate_menu already read the menu from the JOURNAL rather than the graph,
which is the only reason a synthetic {loop}/{i}/__gate__ node — a node
that exists in no graph — can be decided at all. It now returns which
KIND published, because the two menus are not interchangeable: a
HumanGate's options carry a GateOutcome, a loop gate's carry stops, and
appending the wrong event would record a decision the executor cannot
interpret.

The operator's verb vocabulary stays three, not four: both gate kinds are
run gate decide. signal and agent answer refuse, each naming it."
```

---

### Task 13: Cross-process Postgres e2e

**Files:**
- Modify: `crates/torii/tests/e2e_pg.rs`

- [ ] **Step 1: Start a throwaway Postgres**

**Never** use `$DATABASE_URL` — it points at a remote Supabase.

```bash
lsof -i :55432 || echo "port free"
docker run -d --rm --name s4-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:16
until docker exec s4-pg pg_isready -U postgres; do sleep 1; done
```

- [ ] **Step 2: Write the failing test (AC19)**

```rust
/// AC19 — a loop gate awaited in one process, decided through `torii`, resumes and
/// converges in ANOTHER. This is the property the whole durable stack exists for, and
/// the gate is the newest thing in it.
#[tokio::test]
async fn a_loop_gate_decided_in_another_process_resumes_and_converges() {
    let Some(url) = db_url() else { return };
    let db = fresh_schema(&url).await;

    // Process A: drive until the gate pauses.
    {
        let exec = pg_executor_with_human_reviewer(&db).await;
        let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("drives");
        assert!(out.paused.is_some(), "pauses on the human gate: {out:?}");
    }

    // The operator, through torii's own command path.
    {
        let ctx = pg_torii_ctx(&db).await;
        decide(&ctx, run_id(), "lp/0/__gate__", "ship", Some("jerry")).await.expect("decides");
    }

    // Process B: a FRESH executor on the same database.
    {
        let exec = pg_executor_with_human_reviewer(&db).await;
        let out = exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("resumes");
        assert!(out.failed.is_none(), "converges: {out:?}");
        assert!(out.completed.contains(&NodeId("lp".into())), "the Loop completed");
    }
}
```

- [ ] **Step 3: Run**

```bash
DATABASE_URL=postgres://postgres:pg@localhost:55432/postgres \
  cargo test -p sensei-torii --test e2e_pg a_loop_gate_decided_in_another_process
echo "exit=$?"
```

Expected: FAIL first, then **1 passed, exit 0**.

- [ ] **Step 4: Confirm the test is not silently skipped**

Run the same command with `-- --nocapture` and confirm the test **ran** rather than early-returning. `171ccf5` made an unconfigured DB test `ignored` rather than passed — confirm you see `1 passed`, not `1 ignored`.

- [ ] **Step 5: Tear down**

```bash
docker stop s4-pg
```

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/torii/tests/e2e_pg.rs
git commit -m "test(torii): a loop gate decided in another process resumes and converges

Awaited in process A, decided through torii's own command path, resumed
in a fresh process B on the same database. Verified against a throwaway
postgres:16 on a free port, removed afterwards — never against
\$DATABASE_URL, which is remote Supabase."
```

---

### Task 14: Whole-slice verification and review

- [ ] **Step 1: Full suite, real exit code**

```bash
cargo test --workspace; echo "exit=$?"
cargo clippy --workspace --all-targets -- -D warnings; echo "exit=$?"
cargo fmt --all --check; echo "exit=$?"
```

Expected: **exit 0** on all three. Record the actual pass/fail/ignored counts.

- [ ] **Step 2: The Postgres suites, against a real Postgres**

```bash
docker run -d --rm --name s4-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:16
until docker exec s4-pg pg_isready -U postgres; do sleep 1; done
export DATABASE_URL=postgres://postgres:pg@localhost:55432/postgres
cargo test -p sensei-orchestrator-store; echo "exit=$?"
cargo test -p sensei-torii --test e2e_pg; echo "exit=$?"
cargo test -p sensei-orchestrator --features postgres-tests postgres_e2e; echo "exit=$?"
unset DATABASE_URL
docker stop s4-pg
```

Expected: **exit 0**, and **0 ignored** in the DB suites — an ignored DB test means the `have_database_url` cfg did not pick up the variable, so rebuild.

- [ ] **Step 3: Doc-link baseline**

```bash
cargo doc --workspace --no-deps --document-private-items 2>&1 | grep -c 'unresolved link'
```

Expected: the count must not exceed the **24** baseline recorded in `docs/CHECKPOINT.md`.

- [ ] **Step 4: Update the overview**

Add an SP-6 s4 entry to `docs/superpowers/orchestrator-overview.md`'s SP-6 section, and update the s3 entry's carry-forward line — `GateSpec::Agent` is no longer "the obvious next slice", it is **done as `GateSpec::Human`**, and the s3 non-goal that named it should say so.

- [ ] **Step 5: Whole-slice adversarial review**

Run `/sensei:review` (or the `review-slice` skill) over the full diff `git diff origin/main...HEAD`. Every critical/high/medium is fixed **red-first** before the commit gate opens. Re-review after fixing — on this codebase, fixes have repeatedly introduced new defects.

- [ ] **Step 6: Checkpoint and PR**

Update `docs/CHECKPOINT.md` (one current entry, under 40 lines), commit, push, then open a `develop` → `main` PR once CI is green.

---

## Self-review

**Spec coverage** — every AC maps to a task:

| AC | Task | AC | Task |
|---|---|---|---|
| AC1 types + additivity | 1 | AC11 zero spend | 11 |
| AC2 validate_dag | 2 | AC12 resume | 11 |
| AC3 asks per iteration | 6 | AC13 `GateSpec::Agent` refuses | 9 |
| AC4 stop converges | 6 | AC14 model role refused | 9 |
| AC5 continue re-asks | 6 | AC14b unmatched option | 9 |
| AC6 max_iters bounds | 6 | AC15 bounds | 10 |
| AC7 menu from journal | 6 (arm), 12 (torii) | AC16 redaction | 10 |
| AC8 expiry first | 8 | AC17 torii decide | 12 |
| AC9 expiry terminal | 8 | AC18 list-paused | 12 |
| AC10 fails the Loop | 7, 8 | AC19 cross-process | 13 |
| | | AC20 FORMAT_VERSION | 3 |

**Gap found and closed:** AC7 (the menu is read from the journal, not the graph) had no dedicated executor test — Task 6 implements the behaviour and Task 12 tests it at the CLI, but nothing pinned it in the executor. Add this to Task 6's test set:

```rust
/// AC7 — the menu comes from the JOURNAL. Mutating the graph between the ask and the
/// decision must not change what the answer means: an author who flips an option's
/// `stops` after a human picked it would otherwise silently invert their decision.
#[tokio::test]
async fn the_loop_gate_menu_is_read_from_the_journal_not_the_graph() {
    let (exec, journal) = executor_with_human_reviewer_and_journal();
    exec.start(run_id(), &human_gated_loop_graph(3)).await.expect("pauses");
    journal
        .append(run_id(), JournalEvent::LoopGateDecided {
            node: NodeId("lp/0/__gate__".into()),
            option: "ship".into(),
            actor: None,
        })
        .await
        .expect("lands");

    // The author flips `ship` from stopping to continuing, AFTER the human picked it.
    let mutated = Graph {
        nodes: vec![Node {
            id: NodeId("lp".into()),
            kind: NodeKind::Loop {
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "draft it" }),
                gate: GateSpec::Human {
                    agent: AgentRef("reviewer".into()),
                    menu: vec![
                        LoopGateOption { name: "revise".into(), stops: false },
                        LoopGateOption { name: "ship".into(), stops: false }, // flipped
                    ],
                },
                max_iters: 3,
            },
            deps: vec![],
        }],
    };
    let out = exec.start(run_id(), &mutated).await.expect("resumes");
    assert!(
        out.completed.contains(&NodeId("lp".into())) && out.paused.is_none(),
        "the JOURNALED menu still says `ship` stops, so the loop converges — the graph \
         edit must not retroactively change what the human's answer meant: {out:?}"
    );
}
```

**Placeholder scan:** none. Every code step carries real code; the one `…` (Task 6 Step 3's `match` arms) is immediately expanded as a labelled bullet list below it.

**Type consistency:** `LoopGateOption { name, stops }`, `LoopGateStep::{Decided{stop}, Failed, Paused}`, `LoopGateAsk { prompt, menu }`, `LoopGateDecision { option, actor }`, `PublishedMenu::{Human, Loop}`, `human_question_for`, `run_human_loop_gate`, `fail_loop_gate`, `loop_gate_menu_for`, `loop_gate_prompt_for`, `loop_gate_decision_for`. Each is defined in exactly one task and referenced consistently after it.

**Test-helper caveat:** Tasks 6–11 use helpers (`executor_with_human_reviewer_and_journal`, `FakeClock`, `count_node_failed`, `executor_with_human_reviewer_and_counting_gateway`) modelled on the s3 suite's existing fixtures. Task 6 Step 3 should adapt the real ones in `crates/orchestrator/src/executor/tests.rs` rather than write new ones — match the names actually there.
