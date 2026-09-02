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
| `crates/orchestrator/src/executor/agent.rs` | `drive_agent`'s human branch calls the seam | 5 |
| `crates/orchestrator/src/executor/human.rs` | the shared question seam; `run_human_loop_gate` — the whole arm | 5, 6, 8, 9, 10 |
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

    // Pin the BYTES too, not just the round-trip. A symmetric round-trip is invariant
    // under any rename, so it would stay green under `#[serde(rename = "halts")]` on
    // `stops`. The mutation with teeth is `#[serde(default)]` alongside such a rename: a
    // missing field then reads as `false`, so every stopping option in every persisted
    // `scheduled_runs.graph` row becomes NON-stopping and every human-gated loop silently
    // runs to `max_iters` with nobody's answer honoured — verbatim the failure this design
    // exists to prevent. Run it and paste what serde actually emits; do not trust this
    // string unverified.
    assert_eq!(json, r#"{"Human":{"agent":"reviewer","menu":[{"name":"keep-going","stops":false},{"name":"good-enough","stops":true}]}}"#);
}

/// AC1 — additivity: a graph using no `Human` gate serialises exactly as it does
/// today. Guards against a change to the TAGGING REPRESENTATION — `#[serde(tag = …)]`,
/// `untagged`, a rename — silently rewriting every existing `scheduled_runs.graph` row.
///
/// It does NOT catch variant REORDERING, and that is not a gap: externally-tagged serde
/// keys JSON by variant NAME, so order cannot affect the output or name-matched
/// deserialisation. Order would only matter under `untagged` or an index-based binary
/// format, and this workspace persists `Graph` as JSON/jsonb everywhere — no crate pulls
/// a binary serde format. If one is ever added, this test becomes insufficient and a
/// round-trip through THAT format is what would be needed.
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
                // SP-6 s4: the real arm lands in Task 7. Temporary — but a FAILURE, not a
                // panic. `GateSpec::Human` is a public re-exported variant, so it is
                // reachable from any caller and from a `scheduled_runs.graph` jsonb row;
                // `unreachable!` would assert something false. A panic here unwinds through
                // `Scheduler::tick`, which has already claimed a batch and taken its leases
                // — the run stays `'waking'`, the next worker reclaims the stale lease and
                // dies the same way, and because a panic is not an `Err` it bypasses
                // `worker serve`'s consecutive-failure backoff entirely. `graph.rs:548` and
                // `tick`'s own comment both record that doctrine. A silent `false` is worse
                // still: the loop would run to `max_iters` with nobody ever asked.
                GateSpec::Human { .. } => {
                    let msg = format!(
                        "loop {:?}: a human loop gate is not yet wired (SP-6 s4, Task 7)",
                        loop_node.id
                    );
                    return self.fail_loop(run, &loop_node.id, msg).await;
                }
```

and delete it in Task 7. Do **not** leave it past Task 7. This mirrors the gate-agent-failure arm three lines above (`fanout.rs:558`), which is the shape this file already uses for "the gate could not decide".

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
- Modify: `crates/orchestrator-core/src/graph.rs` — a new block after `2b-ter` (the `HumanGate` menu block, ends `:575`), plus the `check_menu_option_names` helper the two blocks share
- Modify: `crates/orchestrator-core/src/plan.rs` — `check_agent_refs` gains a `GateSpec` arm (review follow-up, below)
- Test: `crates/orchestrator-core/src/graph.rs` and `crates/orchestrator-core/src/plan.rs` inline tests

This is the payoff for putting the menu on the graph. Block `2b-ter` is the direct precedent; follow its shape and its message style.

**Also in this task (Task 2 review, Minor): the gate's own `AgentRef` must be resolvable.**
`plan::check_agent_refs` matched `NodeKind::Loop { body, .. }` and never looked at `gate`,
so neither `GateSpec::Agent`'s nor the new `GateSpec::Human`'s agent was checked. An
untrusted `Expand` planner could splice `Loop { gate: … agent: "ghost" }`, `feasible`
accepted it, iteration 0 spent a full body's tokens, and only then did `drive_agent`
surface `UnknownAgent` through `?` — a fatal, non-resumable halt, which is precisely what
`feasible` exists to pre-empt. Bind `gate` and check both variants (they share the field);
`GateSpec::Pure` has no ref. Existence ONLY: whether the role's BACKING suits the variant
is a runtime refusal — `GateSpec::Agent`'s (a human-backed role) already exists in
`drive_agent`, and `GateSpec::Human`'s (a model-backed one) is Task 9/AC14 — and the
runtime has to own both anyway, since a hand-authored graph reaches `Executor::start`
without ever passing through `feasible`.

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
///
/// This rule and the two below come from the shared `check_menu_option_names` (Step 3),
/// so each also asserts the error NAMES THE NODE — the shared function is *told* which
/// node it is validating, and a message that lost the id would leave an author with no
/// way to find the offending menu.
#[test]
fn validate_dag_rejects_a_human_loop_gate_with_an_empty_menu() {
    let err = human_gated_loop(vec![]).validate_dag().expect_err("must reject");
    assert!(format!("{err}").contains("no options"), "{err}");
    assert!(format!("{err}").contains("lp"), "must name the node: {err}");
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
    assert!(format!("{err}").contains("lp"), "must name the node: {err}");
}

/// AC2 — an operator could not type it.
#[test]
fn validate_dag_rejects_an_empty_option_name_in_a_human_loop_gate() {
    let g = human_gated_loop(vec![
        LoopGateOption { name: String::new(), stops: true },
    ]);
    let err = g.validate_dag().expect_err("must reject");
    assert!(format!("{err}").contains("empty name"), "{err}");
    assert!(format!("{err}").contains("lp"), "must name the node: {err}");
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

/// AC2 — the rule fires at DEPTH, at every site `validate_dag` descends into.
///
/// It does NOT test that the new block walks the tree: it does not, and is not meant to.
/// Like 2b/2b-bis/2b-ter it iterates `self.nodes` at ONE level. Depth is delivered by the
/// recursive `validate_dag()` calls in block 2c (a `Subgraph` node's graph, a `Loop`'s
/// `Subgraph` body) and block 2d (a `Branch`'s arms and `default`). What this guards is
/// the COMPOSITION: the rule sits inside the body those calls re-enter, AND every descent
/// site still re-enters it. Each site is its own line of code a later edit can drop, and
/// dropping one is invisible to the five top-level tests above — none of them nest.
/// Step 5 mutates each recursion site separately to show all four cases are load-bearing.
#[test]
fn validate_dag_recurses_into_a_nested_bad_human_loop_gate() {
    // Rejected, and rejected FOR THE NESTED GATE — a nested case that merely errors (say,
    // because the wrapper is malformed) would prove nothing about the recursion.
    fn assert_rejects_the_gate(graph: &Graph, what: &str) {
        match graph.validate_dag() {
            Err(OrchestratorError::InvalidGraph(m)) => assert!(
                m.contains("lp") && m.contains("no stopping option"),
                "{what}: rejected, but not for the nested gate's missing stopping option: {m}"
            ),
            other => panic!("{what}: expected InvalidGraph, got {other:?}"),
        }
    }
    let offender = || human_gated_loop(vec![
        LoopGateOption { name: "again".into(), stops: false },
    ]);

    // Wrappers are otherwise well-formed: the outer Loop's own gate is `Pure`, and each
    // Branch Hard-depends on its `on` node — so only the inner menu can be the defect.
    assert_rejects_the_gate(
        &Graph { nodes: vec![Node {
            id: NodeId("sub".into()),
            kind: NodeKind::Subgraph { graph: Box::new(offender()) },
            deps: vec![],
        }] },
        "nested in a Subgraph", // 2c
    );
    assert_rejects_the_gate(
        &Graph { nodes: vec![Node {
            id: NodeId("outer".into()),
            kind: NodeKind::Loop {
                body: LoopBody::Subgraph(Box::new(offender())),
                input: serde_json::json!({}),
                gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                max_iters: 3,
            },
            deps: vec![],
        }] },
        "nested in a Loop body", // 2c
    );
    assert_rejects_the_gate(
        &Graph { nodes: vec![node("on", vec![]), Node {
            id: NodeId("b".into()),
            kind: NodeKind::Branch {
                on: NodeId("on".into()),
                arms: vec![(BranchCond::FieldTrue("go".into()), offender())],
                default: Graph { nodes: vec![] },
            },
            deps: vec![Dep::hard("on")],
        }] },
        "nested in a Branch arm", // 2d
    );
    assert_rejects_the_gate(
        &Graph { nodes: vec![node("on", vec![]), Node {
            id: NodeId("b".into()),
            kind: NodeKind::Branch {
                on: NodeId("on".into()),
                arms: vec![],
                default: offender(),
            },
            deps: vec![Dep::hard("on")],
        }] },
        "nested in a Branch default", // 2d
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator-core human_loop_gate`

Expected: **5 failures** of the 6 tests, each `must reject: called `Result::unwrap_err()` on an `Ok` value`. The sixth — the accept test — passes trivially, which is expected and fine: it is a regression guard, not a red test.

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
        // carries no timeout. Its SLA is the ROLE's `backed_by: human { timeout }`, a
        // registry fact this function cannot see at all (`validate_dag` is pure over the
        // graph and takes no `Registry`). That does NOT mean the deadline arrives
        // pre-bounded: the registry's layer-1 bound is `Registry::validate`, which applies
        // the same `MAX_AWAIT_SIGNAL_TIMEOUT`, and it lives there rather than in
        // `parse_fm_duration` (which is purely syntactic) because an `AgentDefinition` can
        // arrive as a jsonb row from Postgres and never pass through that parser. Layer 2
        // is unchanged and still required: the shared wait path adds with
        // `checked_add_signed`, not `+`.
        for node in &self.nodes {
            let NodeKind::Loop { gate: GateSpec::Human { menu, .. }, .. } = &node.kind else {
                continue;
            };
            // The same three menu rules 2b-ter applies, from the same function — the nouns
            // differ, the rules do not. Before the stopping-option rule, so an empty menu
            // is reported as an empty menu rather than as one that cannot converge.
            check_menu_option_names(
                "the human gate on loop node",
                &node.id,
                menu.iter().map(|o| o.name.as_str()),
            )?;
            if !menu.iter().any(|o| o.stops) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "loop node {:?} has a human gate with no stopping option, so the loop \
                     can never converge however the human answers — it would run to \
                     max_iters and ask a person that many times to no purpose; at least \
                     one option with `stops: true` is required",
                    node.id
                )));
            }
        }
```

**The shared helper.** The empty / empty-name / duplicate trio is NOT written inline here.
The first cut of this task copied 2b-ter's 45 lines and changed the nouns; the review
called it out against this slice's own s2 rule — *"s1's node was SPLIT, not copied … a
second copy is a second place for [defects] to come back"* — since option names are
author/planner free text and any future guard on them (length cap, whitespace-only,
leading `-`) must reach both menus. Add a private free function beside `Graph`, and call
it from 2b-ter as well:

```rust
fn check_menu_option_names<'a>(
    owner: &str,                              // "human_gate node" | "the human gate on loop node"
    node: &NodeId,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), OrchestratorError> { … }
```

- Empty name and duplicate are checked in the loop; the empty-MENU case falls out of
  `seen.is_empty()` after it (an early return means the set is empty only when the input
  was). `owner`/`node` are interpolated only inside the error branches, so a valid menu
  allocates nothing.
- 2b-ter's three messages come out byte-identical (verified by rendering them) — `owner`
  is exactly its old prefix.
- The at-least-one rule stays with each caller: it is the only genuinely per-kind one
  (`outcome == Complete` vs `stops`), and the two messages explain different failures.
  Each caller must invoke the helper FIRST, so an empty menu is reported as an empty menu.
- The `HashSet` is membership-only and never iterated (the 2b-quater determinism rule).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator-core human_loop_gate`

Expected: **6 passed, 0 failed.**

- [ ] **Step 5: Mutation-check the nesting test**

**Corrected after the Task 2 review — the first version of this step did not test what it claimed.** It said the nesting test guards against "the block could be written to walk only the top level", and prescribed mutating the new block's `for node in &self.nodes` to `for node in self.nodes.iter().take(0)`. Both halves were wrong. The block IS top-level-only, exactly like 2b/2b-bis/2b-ter; depth comes from the recursive `validate_dag()` calls in blocks **2c** (a `Subgraph` node's graph, a `Loop`'s `Subgraph` body) and **2d** (a `Branch`'s arms, its `default`). And disabling the whole block reddens all five rejection tests together, so it proves the block runs at all — nothing about nesting. A vacuous nesting test would have passed that check.

The discriminating mutation disables ONE recursion site at a time and asserts that only the matching case of the nesting test reddens. Each of these was run:

| Mutation (in `validate_dag`) | Expected |
| --- | --- |
| 2c: `NodeKind::Subgraph { graph } => graph.validate_dag()?` → no-op | nesting test fails at case 1 "nested in a Subgraph"; **no other s4 test** fails (4 pre-existing depth tests do) |
| 2c: `LoopBody::Subgraph(graph) => graph.validate_dag()?` → no-op | fails at case 2 "nested in a Loop body"; no other s4 test |
| 2d: `for (_, g) in arms { g.validate_dag()?; }` → no-op | fails at case 3 "nested in a Branch arm" |
| 2d: `default.validate_dag()?` → no-op | fails at case 4 "nested in a Branch default" — and **nothing else in the crate**, so that case is currently the only guard on it |

Run: `cargo test -p sensei-orchestrator-core --lib` under each mutation (the whole crate, not just the one test — "which OTHER tests redden" is the discriminating half), and revert each before applying the next.

The nesting test itself is table-driven over all four shapes (`assert_rejects_the_gate(&graph, what)`, mirroring `validate_dag_recurses_into_a_nested_human_gate` and `validate_dag_rejects_a_path_separator_in_an_author_supplied_node_id`), because AC2 says "including inside a nested `Subgraph`/`Loop` body" — the `Subgraph` node alone is half of it.

- [ ] **Step 6: Restore the doc pointer Task 1 correctly omitted**

Task 1's `GateSpec::Human` doc comment ends *"…so `validate_dag` can reject a menu that
cannot converge."* with no pointer, because the block did not exist yet. It does now —
append `See the `GateSpec::Human` block in [`Graph::validate_dag`].` so the type doc points
at its own validation. Without this the two drift silently: the type claims a guarantee and
nothing links to the code that provides it.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/plan.rs
git commit -m "feat(core): validate_dag rejects a human loop gate that cannot converge

A menu with no stops:true option is a loop that provably cannot converge
however the human answers — it runs to max_iters having asked a person
that many times to no purpose.

Catching this statically is the entire reason the menu lives on the GRAPH
rather than the AgentDefinition: a registry menu is invisible to
validate_dag, exactly as s3's §5.5 records for the human backing itself.

The converse is deliberately NOT checked: an all-stopping menu is
degenerate but legitimate, and rejecting it would be policy, not
structure. The rule reaches every nesting level through blocks 2c's and
2d's recursive validate_dag() calls, and the nesting test mutates each of
those four sites separately to show all four cases are load-bearing."
```

**What actually landed, and the correction on top.** The first commit (`7cf61a5`) carried
the earlier wording — *"Block 2c's recursion carries the rule to every nesting level; the
nested test is mutation-proven, not assumed"* — and both clauses overclaimed: 2c does not
recurse into a `Branch` (2d does), and the mutation Step 5 then prescribed could not
distinguish a vacuous nesting test from a real one. Commit messages are not rewritten here,
so the correction is a follow-up commit that fixes the code comments, the test, and this
plan. When Task 14 traces AC2, read this step's message, not `7cf61a5`'s.

---

### Task 3: The journal variants

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs` — the `JournalEvent` enum, after the `AgentAnswered` variant
- Modify: `crates/orchestrator/src/executor/tests.rs` — two arms in the `label` helper (see Step 5; this is not optional and not Task 4's work)
- Test: `crates/orchestrator-core/src/journal.rs` inline tests

- [ ] **Step 1: Write the failing tests**

> **Corrected after Task 3's review.** The text below is the SHIPPED version. The draft it
> replaces asserted the decided half with `matches!(.., LoopGateDecided { .. })` — the variant
> TAG only — and built the awaited half with `deadline: None`, which `assert!(deadline.is_none())`
> cannot tell apart from a dropped field because `None` is serde's default. Three reviewers
> independently mutation-proved it: `#[serde(skip)]` on `LoopGateDecided::option`, on its `actor`,
> and on `LoopGateAwaited::deadline` each left the draft GREEN. Assert every field BY VALUE, as the
> three sibling round-trips in the same module already do.

```rust
/// AC20 — the durable format version is PINNED, so it cannot move unannounced.
/// (Doc comment abridged here; the shipped one records that this cannot observe
/// additivity — the two old-JSON decode tests do that — and that NOTHING in
/// `orchestrator-core` notices a variant being ADDED.)
#[test]
fn the_durable_journal_format_version_is_pinned_at_1() {
    assert_eq!(FORMAT_VERSION, 1, "the durable journal format version moved. …");
}

/// The two variants round-trip, carrying everything an operator needs to see the
/// question, the menu, the deadline and the decision off the journal alone.
#[test]
fn the_loop_gate_events_round_trip() {
    let asked_at = chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).expect("a valid instant");
    let awaited = JournalEvent::LoopGateAwaited {
        node: NodeId("lp/0/__gate__".into()),
        deadline: Some(asked_at),
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
            assert_eq!(menu[0].name, "done"); // the name is what `--option` matches
            assert!(menu[0].stops);
            assert_eq!(deadline, Some(asked_at)); // the exact instant, not `is_some()`
        }
        other => panic!("wrong variant: {other:?}"),
    }

    let decided = JournalEvent::LoopGateDecided {
        node: NodeId("lp/0/__gate__".into()),
        option: "done".into(),
        actor: "jerry".into(),
    };
    let json = serde_json::to_string(&decided).expect("serialises");
    match serde_json::from_str::<JournalEvent>(&json).expect("deserialises") {
        JournalEvent::LoopGateDecided { node, option, actor } => {
            assert_eq!(node.0, "lp/0/__gate__");
            assert_eq!(option, "done");
            assert_eq!(actor, "jerry");
        }
        other => panic!("wrong variant: {other:?}"),
    }

    // An UNATTRIBUTED row must FAIL to decode — the guard on the variant's contract
    // that an approval always records who claimed to give it. Hand-written JSON: the
    // shape being pinned is one the type can no longer construct, which is the point.
    // One `#[serde(default)]` turns the refusal into a silent `""`, so this is a real
    // guard, not a restatement of serde's defaults.
    let unattributed = r#"{"LoopGateDecided":{"node":"lp/0/__gate__","option":"again"}}"#;
    let err = serde_json::from_str::<JournalEvent>(unattributed)
        .expect_err("an approval with no attribution must not decode");
    assert!(err.to_string().contains("actor"), "…names the missing field: {err}");

    // …and `""`, the degenerate value still expressible, round-trips VERBATIM.
    // Resolving an unnameable actor is torii's job at the WRITE side (`actor_or`); a
    // reader inventing one would launder a writer bug into a plausible audit row.
    let claimed_empty = JournalEvent::LoopGateDecided {
        node: NodeId("lp/0/__gate__".into()),
        option: "again".into(),
        actor: String::new(),
    };
    let json = serde_json::to_string(&claimed_empty).expect("serialises");
    match serde_json::from_str::<JournalEvent>(&json).expect("deserialises") {
        JournalEvent::LoopGateDecided { option, actor, .. } => {
            assert_eq!(option, "again");
            assert_eq!(actor, "", "an empty actor is preserved, never re-labelled");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}
```

> **Corrected out of band, between Tasks 4 and 5 — `actor` is a required `String`, not an
> `Option<String>`.** (No task of this plan owns the change; it is a journal-shape change, cheap
> only while nothing writes the event, so it could not wait for a task that needed it.) The
> version Task 3 shipped had `actor: Option<String>`, following design §4, which specified it
> and never argued for it. It contradicts the reasoning the same spec uses in §3's "Expiry vs
> decision" row: reading expiry before the decision is justified on the ground that answering
> `continue` **authorizes another iteration of spend**, which is an approval in the strict sense
> s2 built its ordering for — and s2 made `GateDecided.actor` a required `String` precisely
> because an approval always records who claimed to give it. No journal holds a `None` to migrate,
> since Task 6 is the first writer. The actor-less round-trip case the draft carried asserted that
> an anonymous decision is a legal shape; that premise is what the narrowing removes, so it is
> REPLACED above (not deleted) by the two properties that survive: an absent `actor` must fail to
> decode, and `""` must round-trip verbatim. Design §4 records the same reasoning.

**Mutation-prove it before moving on.** Apply `#[serde(skip)]` to each of the three fields in
turn and confirm the test reddens each time; the draft above did not. This is a standing
obligation for the slice, alongside Task 2's nested validation and Task 8's expiry ordering.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sensei-orchestrator-core loop_gate_events`

Expected: **compile error** — `no variant named `LoopGateAwaited``.

- [ ] **Step 3: Add the variants**

In `journal.rs`, after the `AgentAnswered` variant. The shipped doc comments are longer than
the sketch below; read them in `journal.rs` rather than re-deriving them from here. Three
claims in the original draft were **wrong** and the review replaced them — do not reintroduce
them:

1. **The drift vectors.** The draft named three ways a graph can be edited between the ask and
   the decision — "a `scheduled_runs.graph` row, a resubmitted `run submit`, or a runtime
   `Expand` subgraph" — and two are false. A resubmit is refused (`cmd::run::submit`'s
   `Scheduler::status` pre-check, and `SchedulerStore::enqueue`'s `on conflict do nothing` +
   `rows_affected == 0` as the real guard); an `Expand` subgraph is the one path that IS bound
   (`PlanExpanded` journals it before it is driven and `drive_expand_with` reuses
   `fold.expansions`). Use s2's GENERAL form — nothing binds the graph handed to a later
   `Executor::start`, there is no graph fence — and cite `scheduled_runs.graph` plus a direct
   embedder `start` as the vectors that hold. Same correction applied to design §5.3.
2. **Why not `GateDecided`.** The draft said `GateDecided` "would additionally carry a
   `GateOutcome` this kind cannot interpret". It carries no `GateOutcome` — it is
   `{node, option, actor, note}`, and the outcome lives on the MENU (`GateAwaited.options`).
   The real reason is the one §3's table and design line 65 give: the two menu VOCABULARIES are
   not interchangeable (`GateOption.outcome` is `{Complete, Fail}`; `LoopGateOption.stops` is
   the continue/stop axis), so a `GateDecided` at a loop-gate node folds into
   `Fold::gate_decisions` and is validated against the wrong menu — which is exactly the
   cross-kind refusal Tasks 9 and 12 must enforce. Note too that `GateDecided` is the ONE
   alternative that does carry an option name, so "all three bypass the menu match" is also
   wrong as stated.
3. **The `actor` `Option`.** The draft justified it with "a loop gate can legitimately be
   decided by an automated operator on a schedule". An automated operator has a name, and s2
   already solved that with a required `String` plus `cmd::gate::actor_or`/`actor_or_user`,
   which never yield an empty actor ("an unresolvable actor is named `unknown`", because a
   blank audit row is indistinguishable from a bug) — so no operator-facing path could produce
   a `None` for that prose to be about. The review struck the justification but left the
   `Option` design §4 specified; the field was then promoted to a required `String` out of
   band, ahead of Task 6, the first writer. **The variant therefore ships as `actor: String`,
   and there is no "what `None` means" paragraph to write** — do not reintroduce either half.
   The reasoning is in the correction blockquote at the end of Step 1.

The variants themselves:

```rust
    LoopGateAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: String,
        menu: Vec<crate::graph::LoopGateOption>,
    },
    LoopGateDecided {
        node: NodeId,
        option: String,
        // REQUIRED, not `Option<String>` — see the correction blockquote in Step 1.
        actor: String,
    },
```

`LoopGateAwaited`'s doc must also carry the two writer obligations `AgentAwaited`'s does, since
this variant's `prompt` is the same model-equivalent question: the **two-cap bound** (authored
half loud against `MAX_HUMAN_TEXT_BYTES`, `## Context` half truncated against
`MAX_HUMAN_CONTEXT_BYTES`) and **redact before appending**. `LoopGateDecided.actor` carries the
redaction obligation too (design §6 — "the leak s3's review caught on that exact field"). Task 10
implements them; the doc states them here so Task 6's append site cannot ship without them.

- [ ] **Step 4: Extend the scrape-sanity assertion**

The `variants.len() > 10 && variants.contains(&"GateAwaited")` check in
`no_doc_comment_links_a_journal_event_variant_by_its_bare_name` is a **scrape sanity check**, not
a variant census — it is a LOWER bound (24 variants against a bound of 10) and cannot notice a
variant being added. Add `variants.contains(&"LoopGateAwaited")` as a second sentinel, and fix
the assertion MESSAGE, which said only "variant scrape broke": a legitimately removed sentinel
now fails it too, and the message must say so.

- [ ] **Step 5: Run to verify passing**

```bash
cargo test -p sensei-orchestrator-core loop_gate; echo "exit=$?"
cargo check --workspace --all-targets; echo "exit=$?"
```

Expected: the tests pass, and `cargo check --all-targets` fails with **exactly one** `E0004`
non-exhaustive-match error, at the `label` helper in `crates/orchestrator/src/executor/tests.rs`.
Add the two arms there — that is Task 3's work, not Task 4's; no later task adds them.

> **`cargo build --workspace` is the WRONG check here and the draft of this step said to use
> it.** Verified at the reviewed commit: with the two `label` arms deleted, `cargo build
> --workspace` exits **0** while `cargo check --workspace --all-targets` exits **101**. The
> `label` helper is the workspace's only exhaustive `match` on `JournalEvent`, and it lives in a
> `#[cfg(test)]` module that `cargo build` never compiles. `fold_journal`'s `_` catch-all is not
> a guard — it absorbs an unknown variant silently — and nothing in `orchestrator-core` notices
> a variant being added at all. A literal execution of the draft would have committed a tree
> where `cargo test --workspace` does not compile.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(core): LoopGateAwaited / LoopGateDecided

New VARIANTS, so FORMAT_VERSION stays 1 — the additivity s3 proved with
AgentAwaited/AgentAnswered.

The menu is journaled for s2's reason, which transfers exactly: nothing
binds the graph handed to a later Executor::start to the one the human was
shown, and an operator's answer must keep meaning what it meant. Reading
the graph's menu at decision time would let an author flip an option's
stops after a human picked it and silently invert their decision.

Not GateDecided: its menu is a different vocabulary. GateOption carries
{Complete, Fail}; a loop gate's option carries stops, so a decision
recorded as GateDecided at a loop-gate node folds into gate_decisions and
is validated against the wrong menu.

Also two arms in the executor tests' \`label\` helper. That labeler is the
only match on JournalEvent in the workspace that enumerates every variant,
and it was the single error \`cargo check --all-targets\` produced —
\`cargo build\` never compiles it, and fold_journal's \`_\` catch-all
absorbed both variants silently, so the fold is not the guard."
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
///
/// The s4 twin of `gate_decisions_are_last_wins_and_the_menu_is_first_wins` (s2) and
/// `agent_answers_are_last_wins_and_the_prompt_is_first_wins` (s3): assert the same
/// three things they do, INCLUDING the deadline. A LAST-wins deadline on the SHARED map
/// is the never-expires bug — a run force-woken every ten minutes under a one-hour SLA
/// re-arms it on every drive.
#[test]
fn the_loop_gate_fold_is_first_wins_for_the_menu_and_last_wins_for_the_decision() {
    let node = NodeId("lp/0/__gate__".into());
    let events = vec![
        (1, JournalEvent::LoopGateAwaited {
            node: node.clone(),
            deadline: Some(at(1_000)),
            prompt: "first question".into(),
            // TWO options with DIFFERING `stops`: a one-option fixture cannot catch a
            // truncating or name-blanking fold, and cannot exercise the realistic menu
            // (Task 2 validates convergence, i.e. a mix of `stops`).
            menu: vec![lopt("done", true), lopt("again", false)],
        }),
        (2, JournalEvent::LoopGateAwaited {
            node: node.clone(),
            deadline: Some(at(9_999)),
            prompt: "second question".into(),
            menu: vec![lopt("done", false)],
        }),
        // DIFFERENT options, so the last-wins assertion pins the name too.
        (3, JournalEvent::LoopGateDecided {
            node: node.clone(),
            option: "again".into(),
            actor: "a".into(),
        }),
        (4, JournalEvent::LoopGateDecided {
            node: node.clone(),
            option: "done".into(),
            actor: "b".into(),
        }),
    ];
    let (fold, _, _) = fold_journal(&events);

    assert_eq!(
        fold.deadline_for(&node),
        Some(Some(at(1_000))),
        "FIRST ask wins — a later one must not push the deadline forward; \
         overwriting it IS the never-expires bug"
    );
    let menu = fold.loop_gate_menu_for(&node).expect("menu folded");
    assert_eq!(menu.len(), 2, "FIRST menu wins — not the later one-option ask");
    assert_eq!(menu[0].name, "done", "the option NAME survives, in order");
    assert!(menu[0].stops, "FIRST menu wins: the second ask must not flip `stops`");
    assert_eq!(menu[1].name, "again", "the whole menu, not just its head");
    assert!(!menu[1].stops, "per-option `stops`, not a single flag");
    assert_eq!(
        fold.loop_gate_prompt_for(&node).expect("prompt folded"),
        "first question",
        "FIRST prompt wins"
    );
    let decision = fold.loop_gate_decision_for(&node).expect("decision folded");
    assert_eq!(decision.actor, "b", "LAST decision wins");
    assert_eq!(decision.option, "done", "LAST decision's option, not the first");
}

/// The fold copies `actor` VERBATIM and never launders a degenerate one.
///
/// REPLACES `a_loop_gate_decision_without_an_actor_folds_as_none_not_as_empty`, whose
/// premise (`None` vs `Some("")`) the `actor` narrowing removed — see the correction
/// blockquote at the end of Task 3 Step 1.
/// What survives is not automatic: `""` is still expressible, and a "helpful" fold —
/// `if actor.is_empty() { "unknown".into() }` — would mirror what `cmd::gate::actor_or`
/// legitimately does at the WRITE side. Doing it HERE is laundering: a row that reads
/// `""` (the CLI was bypassed) would display identically to one written THROUGH
/// `actor_or` (the operator could not be named), collapsing two different failures into
/// one audit line.
#[test]
fn a_loop_gate_decisions_actor_folds_verbatim_including_an_empty_one() {
    let claimed_empty = NodeId("lp/0/__gate__".into());
    let named = NodeId("lp/1/__gate__".into());
    let (fold, _, _) = fold_journal(&[
        (1, JournalEvent::LoopGateDecided {
            node: claimed_empty.clone(), option: "done".into(), actor: String::new(),
        }),
        // Catches what the assertion above CANNOT: a fold that blanks the actor
        // regardless of input agrees with an expectation of `""`. (The laundering
        // substitution reddens that assertion unaided — mutation-proven. This value is
        // `unknown` because that is the string the laundering bug would invent.)
        (2, JournalEvent::LoopGateDecided {
            node: named.clone(), option: "done".into(), actor: "unknown".into(),
        }),
    ]);
    assert_eq!(
        fold.loop_gate_decision_for(&claimed_empty).expect("decided").actor, "",
        "never re-labelled `unknown` — that is what a WRITER through `actor_or` stores",
    );
    assert_eq!(
        fold.loop_gate_decision_for(&named).expect("decided").actor, "unknown",
        "…and the two stay distinguishable from each other",
    );
}

/// `LoopGateAwaited` is the FOURTH writer of the SHARED `deadlines` map, so
/// "has this node begun asking?" still has one answer for every waiting kind. The
/// `None` is folded THROUGH — dropping it is the re-ask-every-drive bug s1 shipped.
///
/// And it writes ONLY its own kind-specific record: if a loop gate leaked into
/// `agent_prompts`, `run_human_agent`'s missing-question arm — which exists to fail loud
/// when a node bears ANOTHER kind's awaited record — would instead resume a human-backed
/// `Agent` with a loop gate's question.
#[test]
fn a_deadline_less_loop_gate_records_that_it_began_asking() {
    let node = NodeId("lp/0/__gate__".into());
    let (fold, _, _) = fold_journal(&[(1, JournalEvent::LoopGateAwaited {
        node: node.clone(),
        deadline: None,
        prompt: "q".into(),
        menu: vec![lopt("done", true)],
    })]);
    assert_eq!(
        fold.deadline_for(&node),
        Some(None),
        "the key must be PRESENT with a None value: present = began asking, \
         None = no deadline"
    );
    assert_eq!(fold.loop_gate_prompt_for(&node), Some("q"));
    assert!(fold.loop_gate_menu_for(&node).is_some());
    // One negative assertion per sibling kind — the other half of the bookkeeping.
    assert!(fold.prompt_for(&node).is_none());
    assert!(fold.menu_for(&node).is_none());
    assert!(!fold.has_signal_ask(&node));
}
```

`fold_journal` takes `&[(Seq, JournalEvent)]` where `Seq` is a bare integer alias and
returns a **3-tuple** — hence `(1, ev)` and `let (fold, _, _) = …`, not `Seq(1)` and a
bare `fold`. `at()` is the module's existing timestamp helper (`support.rs:947`); `lopt`
is its `LoopGateOption` counterpart, added beside the existing `gopt`.

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
    ///
    /// A required `String`, the SAME shape as `GateDecision::actor` and
    /// `AgentAnswer::actor`. The fold cannot widen or narrow it: the event's own field
    /// is required (narrowed out of band — see the correction blockquote at the end of
    /// Task 3 Step 1), so there is no "nobody said who" state left for the side-map to
    /// represent.
    pub(super) actor: String,
}

/// SP-6 s4: a folded `LoopGateAwaited` — what the human was shown.
pub(super) struct LoopGateAsk {
    pub(super) prompt: String,
    pub(super) menu: Vec<orchestrator_core::LoopGateOption>,
}
```

and the three accessors, beside `menu_for`/`prompt_for`. **Each one needs the
`cfg_attr(not(test), expect(dead_code))` block below or the pre-commit hook rejects the
commit** — the fold lands one task ahead of its only non-test consumer, so
`cargo clippy -p sensei-orchestrator --lib -- -D warnings` exits 101 with "methods
`loop_gate_menu_for`, `loop_gate_prompt_for`, and `loop_gate_decision_for` are never
used". This is the s3 precedent, not an invention: `Fold::agent_answer_for`'s doc
(`mod.rs:440`) records the same attribute being carried for one task and then deleted by
its consumer. `expect` rather than `allow` so it deletes itself — an `expect` that is no
longer needed is itself a `-D warnings` failure — and `not(test)` because the fold test
in this module calls all three today, so an ungated `expect` would be UNFULFILLED in the
lib-test target and fail `clippy --all-targets` from the other side.

```rust
    /// The menu a loop gate published, or `None` if it never began asking.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "SP-6 s4 Task 6's run_human_loop_gate is the consumer; the fold lands first"
        )
    )]
    pub(super) fn loop_gate_menu_for(
        &self,
        node: &NodeId,
    ) -> Option<&[orchestrator_core::LoopGateOption]> {
        self.loop_gate_asks.get(node).map(|a| a.menu.as_slice())
    }

    /// The question a loop gate published. Answers "did the LOOP GATE kind begin here?"
    /// — narrower than [`Fold::deadline_for`], which all FOUR waiting kinds write.
    /// (Same `cfg_attr` block; elided here.)
    pub(super) fn loop_gate_prompt_for(&self, node: &NodeId) -> Option<&str> {
        self.loop_gate_asks.get(node).map(|a| a.prompt.as_str())
    }

    /// The decision recorded for a loop gate, if any. (Same `cfg_attr` block.)
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
- Modify: `crates/orchestrator/src/executor/human.rs` (where the seam ended up — see the review
  note at the end of this task; the sketch below says `agent.rs`)
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

**What actually landed (`9286702`), and four deviations from the steps above.**

1. **The fixtures.** Step 1's `executor_with_human_reviewer()` does not exist, as the task's
   own framing warned. The real ones live in `mod human_agent`: `reviewer(timeout, skills)`,
   `human_registry`, `exec_at(&journal, registry, at(1_000))` and `human_gate::at`. The test
   sits in that module, beside the s3 fixtures it reuses.
2. **The assertions are stronger than the sketch.** `question.text().contains("the Acme MSA")`
   plus `timeout.is_some()` is satisfied by a hand-rolled `format!("{system_prompt}: {input}")`
   — i.e. by the second prompt builder this seam exists to prevent. The shipped test also
   asserts an **ACTIVATED skill body** is present (only real assembly produces it) and pins
   the SLA **by value**. (Its mutation evidence was recorded wrongly; see the review
   corrections below, which also strengthen the test again.)
3. **`HumanQuestion::text()` is `#[cfg(test)]`, not `pub(super)`.** A plain `pub(super)`
   accessor is dead code in the lib target and fails the hook's `-D warnings`; more to the
   point, no production caller wants the raw text — every one goes through
   `redact_and_clamp`, and an accessor handing out the UNREDACTED string is exactly the
   shortcut a bypass would take.
4. **A second test ships with the seam: `the_human_question_seam_refuses_a_model_backed_role`.**
   The refusal is new code in this task (unreachable from `drive_agent`, whose branch has
   already matched the backing), so it gets its own red-first guard rather than waiting for
   AC14's end-to-end version in Task 9 — which stays as planned; the two are complementary.
   It also pins that an unknown ref remains `UnknownAgent` rather than being swallowed by the
   new refusal.

**And one correction to Step 3's note.** It anticipates a double *lookup* ("a `HashMap` hit and
clarity wins"); what the extraction actually duplicates on the human path is
`assemble_prompt_parts` as well, since the seam re-assembles. Kept, for a reason the note did
not have: hoisting the assembly below the human branch to avoid it is **not**
behaviour-preserving. Today a non-top-level human role naming an unknown skill fails with
`assemble_prompt_parts`'s `UnknownSkillRef` (a fatal `?`) *before* the `!top_level` refusal is
reached; after the hoist the refusal would win and journal a `NodeFailed` instead.

#### Task 5's review, and what the follow-up commit changed

Three reviewers examined `9286702`. Twelve findings at Minor or above, and **two of the claims
recorded above and in `9286702`'s commit message are false** — the fifth such correction this
slice has had to make (`2d254ad`, `aae15a2`, `6a378f5`, `9e68537`), which is why they are
written down here rather than only in a commit message (`9286702` cannot be amended; the
follow-up commit carries the fix).

**Correction A — the mutation claim in item 2 was wrong.** It said composing from
`agent.system_prompt` instead of `parts.authored` "reddens the skill assertion and nothing else
in the crate". Re-run on a clean tree, `cargo test -p sensei-orchestrator` under that mutation
is **`360 passed; 4 failed`** against `364 passed; 0 failed` clean. The four:

| test | slice |
|---|---|
| `the_human_question_seam_composes_the_same_prompt_the_model_path_would` | s4 (new) |
| `the_journaled_prompt_is_the_assembled_prompt` | s3 |
| `an_oversized_authored_prompt_fails_the_node_before_it_is_journaled` | s3 |
| `the_journaled_question_is_redacted_before_the_durable_write` | s3 |

So the honest reading is the opposite of the one recorded: **the s3 suite is the effective
guard on that mutation**, and the new test earns its place on a DIFFERENT mutation. A
second-prompt-builder inside the seam (concatenate every declared skill, joined with a single
`\n`, ignoring `activation`) — which is precisely the drift the seam exists to prevent —
reddens **only** `the_human_question_seam_composes_the_same_prompt_the_model_path_would`
(`364 passed; 1 failed`), and only after the review strengthened it: the shipped three
`contains` substrings all survive that mutation. The strengthened test computes
`assemble_prompt_parts` itself and requires `question.text()` to contain `parts.authored`
VERBATIM, and registers a second, `OnKeywords`-inactive skill whose body must be ABSENT (with
one `Always` skill, "ACTIVATED" was an unchecked word).

**Correction B — the regression-guard count in `9286702`'s message was the POST-commit
number.** It says "the 21 s3 human-agent tests are the regression guard". `cargo test -p
sensei-orchestrator human_agent` gives **19** at `9286702^` (18 in `mod human_agent` plus
`executor::support::tests::a_deadline_less_human_agent_records_that_it_began_asking`, which the
filter also matches) and **21** at `9286702` — two of which this commit added, so they guard
nothing about it. The s3 regression guard is **19**; the post-commit filter count is 21, and
after the review's own test it is 22.

**The review also closed a real gap, red-first.** The ordering the "one correction" paragraph
above argues for — `UnknownSkillRef` beating the `!top_level` refusal — had **zero** test
coverage anywhere in the workspace. A reviewer applied the hoist and `cargo test --workspace`
exited 0 while the observable behaviour flipped: `Err(UnknownSkillRef)` aborting the run with
no journaled verdict became `Ok` plus a durable `NodeFailed` at the child node — which
`gate_precheck_by_id` then reads back forever.
`an_unknown_skill_beats_the_non_top_level_refusal`
now pins it, and it is the ONLY test that reddens under the hoist (`364 passed; 1 failed`).
That is the class of defect this slice keeps producing: an invariant guarded by the comment
that describes it, which a cleanup pass deletes together with the code.

**Two more, both moves rather than fixes:**

- The seam **lives in `human.rs`, not `agent.rs`.** It is wholly a human-path function and its
  second caller is in `human.rs`; leaving it in `agent.rs` falsified both module headers at
  once (`agent.rs`: "the durable ReAct loop and its per-turn tool execution"; `human.rs`: "a
  new file rather than more of `agent.rs` … `agent.rs` is the model path and stays that").
  The file-structure table above now says `human.rs` for Task 5.
- The seam test's `assert_eq!(calls.len(), 0)` is **gone.** It could not fail:
  `human_question_for` is a synchronous `fn`, the `CallLog` is written only by
  `RecordingAdapter::chat`, and the test drives no node — under the mutation above it stayed
  green while the prompt assertion reddened. It is the same unfalsifiable shape `mod
  human_agent`'s own doc records as an s2 defect. Zero spend is a property of each CALLER's
  structure, so **AC11's end-to-end test owns it**, over a run that really could have spent.

---

### Task 6: `run_human_loop_gate` — ask, and honour a decision

**Files:**
- Modify: `crates/orchestrator/src/executor/human.rs`
- Test: `crates/orchestrator/src/executor/tests.rs`

The core arm. Structure follows `run_human_gate` (s2), **not** `run_human_agent` (s3) — the expiry ordering is the difference and Task 8 pins it.

**Delete the three `cfg_attr(not(test), expect(dead_code))` blocks Task 4 put on
`Fold::loop_gate_menu_for`/`loop_gate_prompt_for`/`loop_gate_decision_for` as you wire each
one up.** That is not tidying: an `expect` whose lint no longer fires is itself a
`-D warnings` failure, so the pre-commit hook rejects the commit that reads them while the
attribute is still there. s3 did exactly this at its own Task 4 and left the reason in
`Fold::agent_answer_for`'s doc; replace each attribute with the same one-paragraph note
rather than deleting it silently.

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
            actor: "jerry".into(),
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
            actor: "jerry".into(),
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
                actor: "jerry".into(),
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
        //
        //    **The iteration output goes in the `context` argument, NOT `input`.** See
        //    below — this is the one call in the slice where getting it wrong is a
        //    terminal, data-dependent node death.
        let ask = gate_ask(menu);
        let context = [(ContextKey("iteration output".into()), iteration_output.clone())];
        let (question, timeout) =
            match self.human_question_for(agent_ref, &serde_json::json!(ask), &context) {
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

> **Which seam argument the iteration output goes in — corrected by Task 5's review, before
> this task was executed.**
>
> This sketch originally read `self.human_question_for(agent_ref, iteration_output, &[])`, and
> that is a **defect**, not a style choice. `HumanQuestion::compose` puts the seam's `input`
> into the `## Task` tail and counts it in `authored_bytes`
> (`let authored_bytes = text.len() + task.len()`), and the `NotYetAsking` arm below checks
> `authored_bytes` against `MAX_HUMAN_TEXT_BYTES` (4096) with a **loud terminal
> `NodeFailed`**. A loop gate's iteration output is a model answer, i.e. over 4 KiB
> essentially always — design §6 says exactly that — so the gate would die on ordinary run
> data, *after* the iteration's tokens were spent, unrecoverably (`gate_precheck_by_id` reads
> the `NodeFailed` back on every later drive), blaming the config author's system prompt.
> That is verbatim the defect s3's whole-slice review found and the two-cap rule exists to
> prevent; `an_oversized_node_input_fails_the_node_before_it_is_journaled` already proves a
> >4096-byte `input` kills the node. It also contradicted design §2 ("the person sees the
> iteration output as `## Context`"), §6, and this plan's own AC15 test
> `a_verbose_iteration_output_truncates_the_question_instead_of_killing_the_gate`, which
> asserts `out.failed.is_none()` on a ~50 KiB output. The sketch was internally inconsistent
> too: the `NotYetAsking` arm tells the failure message to say "`## Context` is **not**
> counted", which only makes sense if the output were in `## Context`.
>
> **The rule, now also on `human_question_for`'s own doc comment so the next caller cannot
> pick wrong:** `input` is the ASK (author-scale, charged to the LOUD 4096 cap, and the query
> `activation.is_active` is evaluated against); `context` is RUN DATA (truncated per
> dependency to `MAX_HUMAN_CONTEXT_BYTES`, 32 KiB, and excluded from `authored_bytes`).
>
> **So what is the `## Task` for a gate?** A short ask synthesized from the menu — a pure
> `gate_ask(menu) -> String` along the lines of *"Review the iteration output above and choose
> one: `revise`, `accept`."* Two reasons for that shape rather than a bare constant. It keeps
> everything charged to the loud cap **author-controlled at config time**, which is the whole
> principle behind which half fails loudly: the menu names are author free text and their
> being over 4 KiB really is a config error the author can act on. And it makes the journaled
> `LoopGateAwaited.prompt` self-contained — `torii run list-paused` renders the `menu` field
> beside it, but the durable question should still state what is being decided, the same
> reason s3 added `## Task` at all (§5.4: never show the human LESS than the model would
> have had).
>
> **One consequence to accept deliberately:** activation is then evaluated against the ask,
> not against the iteration output, so a gate role's `OnKeywords` skills match on the menu
> text. That is a live limitation, not an oversight — the seam has ONE `input` serving both
> `## Task` and the activation query, and splitting it into two parameters is a change to
> s3's path as well. `Always` skills (the default, and what a gate role wants) are unaffected.
> If a real gate role ever needs output-driven activation, that is the change to make, and it
> gets its own red-first commit.

with these arms:

- `Err(message)` → `fail_loop_gate(run, node_id, format!("loop_gate: {message}"))`.
- `Ok(WaitState::Expired(d))` → fail, naming the deadline and **not** "no decision" (this arm has not read the fold and cannot know whether one exists — the exact wording bug s2's review caught).
- `Ok(WaitState::NotYetAsking(fresh))` → bound `question.authored_bytes` against `MAX_HUMAN_TEXT_BYTES` (loud fail, message naming the role's `system_prompt`/skills **and the menu names**, since `gate_ask(menu)` is the third author-controlled contributor to that count, and stating that `## Context` — the ITERATION OUTPUT — is **not** counted), then `question.redact_and_clamp(|t| self.redact_text(t), MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES)`, append `LoopGateAwaited { node, deadline: fresh, prompt, menu: menu.to_vec() }`, return `Paused`.
- `Ok(WaitState::Waiting(d))` → read the **journaled** menu via `fold.loop_gate_menu_for(node_id)`. Absent ⇒ a kind-swapped node (the shared `deadlines` map has four writers) ⇒ fail loudly, never fall back to the graph's menu. Then read `fold.loop_gate_decision_for(node_id)`; absent ⇒ `Paused`; present ⇒ match `option` against the journaled menu; unmatched ⇒ fail loudly (Task 9); matched ⇒ `Decided { stop: o.stops }`.

Add `fail_loop_gate`, mirroring `fail_human_agent`, so this arm cannot skip the redaction chokepoint. Add `gate_ask(menu) -> String` as a free `fn` in `human.rs` — pure, so it can be unit-tested without an executor, and so the `## Task` text has one definition rather than being inlined at the append site.

- [ ] **Step 4: Run to verify passing**

Run: `cargo test -p sensei-orchestrator human_loop_gate`

Expected: **4 passed, 0 failed** (these will only fully pass once Task 7 wires the arm; run Task 7 first if the gate is still `unreachable!`).

- [ ] **Step 5: Commit** (fold into Task 7's commit if the arm is not yet reachable)

**What actually landed (Tasks 6 and 7 as one commit), and five deviations from the steps
above.** All five are in the same direction: the steps under-specified something the
compiler or a measurement then forced.

1. **`LoopGateStep::Failed` is a STRUCT variant, `{ message, newly_journaled }`, not
   `Failed(String)`.** Measured, not theorised: with the sketch's shape, a gate killed by
   expiry left `run_loop` appending a fresh `NodeFailed` for the LOOP on **every** later
   drive (3 rows after two wakes, growing without bound), because a human gate's verdict is
   terminal — step 0 reads it back forever — while the run it kills journals no
   `RunCompleted` and so stays resumable. Reading the GATE's verdict back was only half the
   fix; the wrapper one level out re-derived its own. That is verbatim the unbounded-growth
   defect `gate_precheck` exists to prevent, and **AC9's test as sketched in Task 8 would
   have failed** (`count_node_failed` is whole-run). The flag is carried on the step rather
   than re-derived in `run_loop` from `Fold::failed`, because that map has exactly ONE
   reader family by design — `gate_precheck` and its `_by_id` forms, on behalf of the
   WAITING kinds — and a `Loop` is not one. `a_dead_loop_gate_stops_appending_node_failed_
   rows_on_every_wake` guards it and is mutation-proven (disable the branch → it is the only
   test in the crate that reddens).
2. **`fail_loop_gate` returns the whole `LoopGateStep`, not a `String`.** That is what makes
   `newly_journaled: true` unforgeable: the only place it is set is the function that writes
   the row. Every failure site is then `return self.fail_loop_gate(…).await;`.
3. **A third form of the shared terminal guard: `Executor::gate_failure_by_id`.** The sketch
   has step 0 call `gate_precheck_by_id`, which returns an `Option<NodeExec>` this kind
   cannot use — leaving either an `unreachable!` (forbidden) or an `if let Some(NodeExec::
   Failed { .. })` whose non-matching path falls THROUGH and silently ignores a recorded
   failure, i.e. fail-OPEN in the one guard whose whole purpose is fail-closed. So
   `gate_precheck_by_id` is now a two-line wrapper over `gate_failure_by_id`, exactly as the
   `&Node` form is a wrapper over it: still ONE read of ONE map.
4. **AC14b's unmatched-option refusal ships HERE, not in Task 9.** The arm cannot compile
   without a branch for it, and the only correct branch is the loud one. Task 9's AC14b test
   will therefore be green on its first run and must be mutation-proven rather than
   red-first; AC13 and AC14's end-to-end tests are untouched and still Task 9's.
5. **Task 7 Step 2's "check whether a documentation test needs to learn about
   `GateSpec::Human`" answered YES, and it was a real gap.**
   `every_node_kind_is_documented_in_the_execution_graph_feature_doc` cannot see it — a gate
   is not a `NodeKind` — so `execution-graph.md` still read "**`GateSpec`** is
   **`Pure(LoopGate)`** … or **`Agent { … }`**", a CLOSED enumeration stating that s4's kind
   does not exist. A sibling guard,
   `every_gate_spec_variant_is_documented_in_the_execution_graph_feature_doc`, was written
   RED (`missing: ["Human"]`), then the paragraph was filled in. Bounded to the enumeration
   sentence for the node-kind guard's own reason: `Human` occurs in that page's prose in
   several unrelated senses, so a bare `doc.contains` would have been green before this
   slice wrote a word.

**Two smaller things.** `Fold::loop_gate_prompt_for` turned out to have no production
consumer — the arm reads `loop_gate_menu_for`, which answers the same "did the LOOP GATE
kind begin here?" question AND returns the value the arm needs — so it is now `#[cfg(test)]`
rather than carrying an `expect(dead_code)` whose stated occasion never arrives (the
`HumanQuestion::text` precedent from Task 5). And the `NotYetAsking` arm PAUSES rather than
falling through to read a decision, which is where s2's shape and s4's differ: a loop gate's
path is synthesized per iteration and cannot be decided before it exists through any
operator surface, so the early-decision race s2 resolves in-execution is unreachable here
and costs at most one extra wake from a hand-written journal.

**One thing that did NOT land and was not recorded: the AC7 executor test.** This plan's
own Self-review found that gap and closed it in writing — "Add this to Task 6's test set",
with the test written out in full — and Tasks 6+7 shipped without it while the deviation
list above enumerated five other things. Whole-slice review found it re-opened and
mutation-proved it unguarded. It is now in `mod human_loop_gate` as
`the_loop_gate_menu_is_read_from_the_journal_not_the_graph`, with one correction to the
sketch: the sketch flips `ship` alone to `stops: false`, and that graph does not survive
`validate_dag` ("no stopping option, so the loop can never converge"), so the shipped test
SWAPS the two options' meanings instead — a legal graph, which is what makes the test about
the menu's durability rather than about validation catching the edit for us.

---

**The Tasks 6+7 whole-slice review, and the Critical it found.** Twenty-three findings at
Minor or above across four reviewers; the code half is recorded here because it changes the
DESIGN, not only this plan.

**Critical — a decided gate was killed by its own stale deadline.** `run_loop` re-enters
`for i in 0..max_iters` from zero on every drive, so iteration 0's gate is re-derived
forever, while the deadline it recorded is fixed at its own ask. Expiry is read first (§3,
and rightly), and `wait_or_expire_by_id` knows nothing about a decision an earlier drive
already read, honoured and spent an iteration against — so the moment wall-clock passed
iteration 0's deadline the whole `Loop` died. Two reproductions: (1) a 3-iteration loop
under a 1h SLA answered at +30m and +70m, both strictly inside their OWN gates' SLAs, failed
at `lp/0/__gate__`; (2) a loop that had already CONVERGED, whose run parked on a downstream
`AwaitSignal`, was destroyed a day later when the signal arrived, cascade-skipping the very
node it was delivered for. Any multi-iteration human-gated loop with a finite SLA was
unusable. Every existing s4 test held the clock at `at(1_000)`, which is why the suite was
green — the guarding tests advance it ACROSS iterations.

The fix is a THIRD journal variant, `LoopGateSettled { node, option }` (design §4, §5.2 step
0b, §5.7, AC12b): the drive that honours a decision records it, and every later drive reads
it back before the clock is consulted. Reordering to read the decision first would fix the
same symptom and reopen AC8 — mutation-checked both ways.

**Deviation 1 above is REVERSED.** `LoopGateStep::Failed` is `Failed(String)` after all, as
the original sketch had it. The `newly_journaled` flag it carried was a second claim about
the journal that could disagree with the first — it meant "this drive wrote the GATE's row"
while `run_loop` consumed it as "the LOOP's row is missing" — so a transient journal error
between `fail_loop_gate`'s append and `fail_loop`'s left the Loop's failure permanently
unwritten, in-process `RunOutcome.failed` disagreeing with `torii run status` and the
`on_node_failed` hook forever. `fail_loop` now takes the `&Fold` and appends only if the
`Loop` has no recorded failure: idempotent, self-healing, and true for all THREE of
`run_loop`'s failure paths rather than special-cased for one. Deviation 2's argument
("`newly_journaled: true` is unforgeable") goes with it; `fail_loop_gate` still returns the
whole step, which is now simply the tidier shape.

**Two bounded-growth defects one edge further out.** `cascade_skip_from` was not
fold-guarded, so a terminally-dead run appended one `NodeSkipped` per hard dependent per
wake (measured 1 / 2 / 3) — the `newly_journaled` fix had bounded the `NodeFailed` rows and
left these growing, while the test named for the property watched one of the six event kinds
it could grow by. `Fold::skipped` now guards the append, and the test asserts over EVERY
journaled row against a fixture that has a dependent.

**Deviation 5's bound was not what its comment said.** The enumeration guard bounded to the
first blank blockquote line, which by then was 46 lines / 3.8 KB away and covered three
further paragraphs about human loop gates — re-creating the `doc.contains` weakness it was
written to avoid. `execution-graph.md` now carries an explicit
`<!-- gate-spec-enumeration -->` marker pair and the test bounds to it.

**Three tests the slice was missing, all mutation-proven, all now in `mod
human_loop_gate`:** the AC7 test above; `a_loop_gate_that_recorded_a_wait_without_a_menu_
fails_loudly`, the fourth kind-swap sibling (s3 shipped its copy missing too, and review
found that one as well); and `a_decision_after_the_deadline_does_not_continue_the_loop`,
which **Task 8 no longer owns** — the arm's own doc named it in the present tense while it
existed only in this plan, so it landed with the fix that needed it as a fence. Task 8
should confirm it still reddens against the s3-shaped hoist and add AC9's remaining
coverage rather than re-writing it.

**Task 12 gains a precondition.** `signal_states` (`torii/src/cmd/run.rs`) folds
`SignalAwaited`/`GateAwaited`/`AgentAwaited` and NOT `LoopGateAwaited`, so a run paused on a
human loop gate is invisible to `run list-paused` and no verb can write a `LoopGateDecided`.
Nothing is mis-delivered (both `run signal` and `run gate decide` see no ask and refuse),
but the arm is live on `develop` and a run that reaches a `GateSpec::Human` today can only
wait for its SLA and be destroyed by it (§7). Close the window before anything authors one.

---

### Task 7: Wire the arm into `run_loop`

**Files:**
- Modify: `crates/orchestrator/src/executor/fanout.rs:545-571`

- [ ] **Step 1: Replace the temporary arm**

Delete the temporary `fail_loop` stub arm from Task 1 — **confirm it is gone, not merely
shadowed**; leaving it beside the real arm is dead code the "do not leave it past Task 7"
rule exists to prevent. Then add:

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

- [ ] **Step 2: Update `run_loop`'s own doc comment**

`fanout.rs`'s `run_loop` doc (around `:427`) enumerates the gate as *"either `Pure` … or
`Agent`"*. Task 1 already added the third clause; confirm it is still accurate against the
final arm, and that nothing else in the file's prose still says there are two.

This is not cosmetic here: the codebase has a test
(`every_node_kind_is_documented_in_the_execution_graph_feature_doc`) built on the principle
that a kind a doc omits is a kind the doc states does not exist. Check whether that test —
or a sibling — also needs to learn about `GateSpec::Human`, and if it does, that is a red
test to write before the arm lands, not a doc chore afterwards.

- [ ] **Step 3: Run the Task 6 tests**

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

> **AC8's test ALREADY LANDED, with the Tasks 6+7 review.** The arm's own doc comment named
> `a_decision_after_the_deadline_does_not_continue_the_loop` in the present tense — "the test
> that reddens when it is [collapsed]" — while it existed only in this plan, so the slice's
> headline safety property was live on `develop` protected by nothing but a comment, and
> review mutation-proved that an s3-shaped hoist landed green. It could not wait, because the
> Critical fix in that same review (`LoopGateSettled`, design §4) touches this exact ordering
> and needed a fence to be reviewed against. The shipped test is in `mod human_loop_gate` and
> covers AC8 plus AC9's terminality and no-second-`NodeFailed` halves; it is mutation-proven
> against the hoist BEFORE and AFTER the Critical fix.
>
> What is left for this task: AC10's own assertion, whatever of AC9 the shipped test does not
> reach, and — new — **AC12b**, the multi-iteration clock-advancing case the Critical fix
> introduced. Two of those tests exist already
> (`a_decision_honoured_inside_its_sla_survives_a_later_iterations_clock`,
> `a_converged_loop_is_not_re_killed_by_its_own_gates_stale_deadline`); this task should
> extend rather than duplicate them, and must keep advancing the clock ACROSS iterations —
> every s4 test written before them held it fixed, which is how the Critical shipped green.

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
            actor: "late".into(),
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
            actor: "too-late".into(),
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

> **A FOURTH refusal belongs here and was never scheduled: the kind swap.** Its three
> siblings each ship one (`a_signal_node_that_recorded_a_wait_without_a_signal_ask_fails_
> loudly`, `a_gate_that_recorded_a_wait_without_a_menu_fails_loudly`,
> `an_agent_node_that_recorded_a_wait_without_a_question_fails_loudly`) — a convention
> `fold_journal`'s writer-list comment reasons from explicitly — and s3 shipped ITS copy
> missing before review found it. s4 repeated that exactly. The Tasks 6+7 review landed
> `a_loop_gate_that_recorded_a_wait_without_a_menu_fails_loudly`, mutation-proven against
> `fold.loop_gate_menu_for(node_id).or(Some(menu))` — the silent fallback to the GRAPH's
> menu that the arm's own comment forbids and that had been shipping green. Nothing is left
> for this task on that refusal; note it here so the convention is scheduled next time.
>
> Since that review the refusal has TWO call sites — the live `Waiting` arm and the settled
> replay (`decide_from_published_menu`, design §5.2 step 0b) — sharing one message builder.
> **Both of the replay site's refusals are now covered**, by `eba6083`'s verify follow-up:
> `a_settled_loop_gate_with_no_published_menu_fails_loudly` and
> `a_settled_loop_gate_naming_an_option_outside_its_menu_fails_loudly`. The replay path was
> the Critical fix's own new code and shipped with neither refusal exercised; both are
> mutation-proven against the silent default
> (`…find(…).map(|o| o.stops).unwrap_or(false)`), which continues the loop off a settlement
> it could not resolve. Nothing is left for this task on the replay half either — AC14b's
> own test still owes the LIVE arm's unmatched-option refusal.
>
> **The kind-swap arm's reachability was restated and is now guarded on the AUTHORED side
> too.** `eba6083`'s comment claimed one vector was a `/`-containing authored id reaching
> the executor because "`Executor::start` takes the graph as an unvalidated caller
> parameter". Both halves are false: `validate_dag` rejects the separator at every depth,
> and `start_inner`/`run_inner` each call it before anything is journaled. The vector that
> DOES hold needs no separator — an author names a `Loop`-body node `__gate__`, and
> `drive_nested` namespaces it onto exactly `"{loop}/{i}/__gate__"`. Guarded by
> `an_authored_gate_id_in_a_loop_body_collides_and_fails_loudly`.
>
> **Follow-up, deliberately NOT taken here (out of scope for a findings commit, and not a
> silent failure): reserve the `__gate__` SEGMENT in `validate_dag`.** `plan::feasible`
> already does (`PlanError::ReservedNodeId`, the SP-3 s5 review's fix), so only the
> author-supplied path is open; s1's rule bans the `/` separator, which makes the whole
> reserved path unauthorable in one piece but says nothing about a bare segment that becomes
> that path once nesting flattens it. Today the collision is a LOUD gate failure whose
> message ("a waiting node's kind cannot be changed mid-run") diagnoses the wrong thing —
> the author named a reserved id. Rejecting it at validation, with a message that says so,
> is the right fix and belongs with Task 14 or a follow-on slice.

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
            actor: "jerry".into(),
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

- [ ] **Step 3: Reconcile the journal-variant docs with what actually shipped**

Task 3's review put these obligations on the variants' own doc comments **ahead of** this task,
deliberately: in this codebase the event's doc is where the next writer of the event learns its
contract, which is why s3's review put the same rules on `AgentAwaited`, and Task 6's append site
lands four tasks before this one. So the doc text already exists — `LoopGateAwaited.prompt`
states the two-cap rule and the redact-then-clamp rule, and `LoopGateDecided.actor` states the
redaction rule ("the leak s3's review caught on that exact field", design §6).

This step is therefore a RECONCILIATION, not an addition: re-read both docs against the shipped
behaviour and correct whichever is wrong. A doc that promises a bound the code does not enforce
is worse than no doc — that is the Task 1 finding pattern this slice keeps hitting. In
particular, confirm that `actor` really is redacted (nothing in Task 10's three tests above
asserts it — add a fourth if the shared seam does not make it structural).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
# plus crates/orchestrator-core/src/journal.rs if Step 3 corrected either variant's doc
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
            actor: "jerry".into(),
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

> **PRECONDITION, from the Tasks 6+7 review.** The executing arm is live on `develop` and
> this surface is not, so the window is open right now: `signal_states` (`run.rs`) folds
> `SignalAwaited`/`GateAwaited`/`AgentAwaited` and **not** `LoopGateAwaited`, so a run paused
> on a human loop gate is INVISIBLE to `run list-paused`, and no verb can write a
> `LoopGateDecided`. Nothing is silently mis-delivered — both `run signal` and `run gate
> decide` see no ask for that node and refuse — but a run that reaches a `GateSpec::Human`
> today can only wait for its SLA and then be destroyed by it, which is §7's sharpest cost
> reached with no operator recourse at all. Close it before anything authors a
> `GateSpec::Human`, and add `LoopGateAwaited` to `signal_states` as the first step, not the
> last. (`LoopGateSettled` needs no operator surface: it is executor bookkeeping, and a
> settled gate is one nobody is waiting on.)

> **SETTLED out of band, between Tasks 4 and 5 — was "carried forward from Task 3's review".** No
> task of this plan owns the change; see the correction blockquote at Task 3 Step 1. The question
> this note used to pose (should `LoopGateDecided.actor` stay `Option<String>` where
> `GateDecided.actor` is a required `String`?) is closed: it was promoted to `String`, because
> the asymmetry had no semantic justification — a loop gate's decider is exactly as attributable
> as a `HumanGate`'s, and s2 deliberately made a blank audit row unrepresentable via
> `actor_or`/`actor_or_user` ("an unresolvable actor is named `unknown`"). It was done then
> rather than here because it is a **journal shape change**, cheap only while nothing has
> written the event, and Task 6 is the first writer. Spec §4 and Task 3 carry the reasoning.
>
> What that leaves for THIS task is narrower but not gone, and both halves still bind:
> **(a)** the shared decide path must route the loop-gate branch through `actor_or_user` too.
> This is now the ONLY thing standing between an operator and a blank audit row: the `Option`
> at least made "nobody said who" legible as such, whereas a `String` that skipped the resolver
> would journal clap's empty default as a silent `""`, indistinguishable at a glance from a
> real name. `decide` (`gate.rs:238`) already takes `actor: &str` and `main.rs:450` already
> resolves it, so the branch inherits this by staying inside that signature — the failure mode
> is a *second* append site added beside it, not the existing one.
> **(b)** the field-type difference is no longer an obstacle to the "factor it over the option
> NAMES" sharing in Step 3, which is the point of having done it: the two events' `actor` fields
> are now the same type, so the shared path needs no `.map(Some)` and no per-kind branch for
> attribution at all.

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

Expected: the count must not exceed the baseline recorded in `docs/CHECKPOINT.md` — **16**, re-measured with this exact command at Task 3. (This step originally said 24, which no invocation of the command above produces; the stale number would have hidden eight new broken links.)

- [ ] **Step 4: Update the overview and the two feature docs**

Add an SP-6 s4 entry to `docs/superpowers/orchestrator-overview.md`'s SP-6 section, and update the s3 entry's carry-forward line — `GateSpec::Agent` is no longer "the obvious next slice", it is **done as `GateSpec::Human`**, and the s3 non-goal that named it should say so.

Two **feature** docs drift with this slice and were missing from this list entirely (found by Task 3's review — the variants alone already falsify the first of the three sentences below):

- `docs/features/orchestrator/durable-journal.md` — the SP-6 s3 section opens "Two variants **complete** the journal's HITL vocabulary, after s1's … and s2's …". There are now four pairs. Task 3 downgraded "complete" to "extend" as a stopgap; this step writes the real s4 section (`LoopGateAwaited` first-wins with its durable menu + question, `LoopGateDecided` last-wins) and the Gherkin scenarios beside the s3 ones. (The `AgentAwaited` bullet's "shared by all three waiting kinds … the only one of the three that can answer" sentence was a second falsified claim in this file; it was **already corrected in the Task 4 review follow-up** and needs no further edit here.)
- **Before closing this step, bound the set rather than trusting these three bullets:** `rg -in 'all (THREE|FOUR|FIVE)|(three|four|five) waiting kinds' crates docs`. The waiting-kind count is restated in production comments, in the rustdoc of the tests that guard the kind-swap arms, in `e2e_pg.rs`, and in both feature docs; Task 4 updated the arms alone and the review found four more stale sites. Several remaining hits (`tests.rs`'s `pause_awaiting` and shared-helper comments, `torii/src/cmd/run.rs`'s three-way cross-refusal) become stale only once Tasks 6–7 add the fourth EXECUTING kind and Task 12 adds its CLI verb — check them then, not before.
- `docs/features/orchestrator/README.md` — two sentences in the **Durable journal** status row: "all six are new variants, so `FORMAT_VERSION` stays 1" (now eight) and `AgentAwaited` described as "the only one of the three waiting kinds that carries a prompt" (`LoopGateAwaited` carries one too, and there is a fourth waiting kind). The row's `Partial (… · SP-6-3)` marker and the **Execution graph** row's `GateSpec` description need the s4 bump as well, and the **HITL (SP-6)** bullet needs its s4 paragraph — including retiring the s3 carry-forward sentence that calls this slice "the obvious next slice".

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
            actor: "jerry".into(),
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

**Type consistency:** `LoopGateOption { name, stops }`, `LoopGateStep::{Decided{stop}, Failed, Paused}`, `LoopGateAsk { prompt, menu }`, `LoopGateDecision { option, actor }`, `PublishedMenu::{Human, Loop}`, `human_question_for`, `run_human_loop_gate`, `fail_loop_gate`, `gate_ask`, `loop_gate_menu_for`, `loop_gate_prompt_for`, `loop_gate_decision_for`. Each is defined in exactly one task and referenced consistently after it.

**Test-helper caveat:** Tasks 6–11 use helpers (`executor_with_human_reviewer_and_journal`, `FakeClock`, `count_node_failed`, `executor_with_human_reviewer_and_counting_gateway`) modelled on the s3 suite's existing fixtures. Task 6 Step 3 should adapt the real ones in `crates/orchestrator/src/executor/tests.rs` rather than write new ones — match the names actually there.
