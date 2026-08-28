# SP-6 s3 — human-as-Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A role in the registry answered by a person instead of a model — an `Agent` node whose `AgentRef` resolves to a human-backed definition pauses once, journals the question, and completes when a human answers.

**Architecture:** `AgentBacking::{Model, Human{timeout}}` on `AgentDefinition` (serde-defaulted to `Model`, so existing configs are byte-identical). `drive_agent` short-circuits between `assemble_prompt` (`agent.rs:75`) and `resolve_chain` (`agent.rs:76`) — so the composed prompt is reused and no gateway call is reachable. Two new journal variants carry the question and the answer; `FORMAT_VERSION` stays 1. The waiting machinery is s2's shared `gate_precheck`/`wait_or_expire`/`pause_awaiting`, not a third copy.

**Tech Stack:** Rust 2024, `tokio`, `chrono`, `serde`/`serde_json`, `clap` 4 derive, `sqlx` (Postgres, feature-gated), `async_trait`.

**Spec:** `docs/superpowers/specs/2026-08-27-sp-6-s3-human-as-agent-design.md`

**Baseline that must not regress:** `env -u DATABASE_URL cargo test --workspace` = **1505 passed / 0 failed / 7 ignored**, exit 0.

---

## Ground rules for every task

- **TDD strictly.** Failing test first, RUN it, watch it fail, then implement. A test that never went red proves nothing. For a guard-style test green on arrival, apply its mutation in a **scratch copy** (`rsync -a --exclude=target --exclude=.git ./ /tmp/s3/`, never the working tree) and confirm red there.
- **Verify real exit codes.** Never `cargo test … | tail` — the pipe's status is not the command's. Run it, read `$?`. zsh: `pipestatus`, not `PIPESTATUS`. `cargo test` accepts only ONE positional filter.
- **Commit via stdin heredoc** (`git commit -F - <<'MSGEOF' … MSGEOF`), NEVER `-m`. zsh command-substitutes backticks in `-m` and has already corrupted a commit message here.
- **Never run anything against `$DATABASE_URL`** — it points at a remote Supabase instance and the DB suite applies its own schema. Always `env -u DATABASE_URL`. For the e2e, start a throwaway container on a free port and remove only that one.
- **`cargo fmt --all` before every commit.** Pre-commit = fmt-check + `clippy -D warnings`, NO tests.
- **Do not amend or rebase** — commit on top.
- Doc comments explain WHY, including what went wrong historically. Terse comments are a defect here. **Verify every factual claim about the codebase before writing it** — this feature has shipped seven confidently-worded false doc claims caught only in review.

## File structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/registry.rs` | `AgentBacking`, the `validate()` rules | Modify |
| `crates/orchestrator-core/src/journal.rs` | `AgentAwaited` / `AgentAnswered` | Modify |
| `crates/orchestrator-core/src/lib.rs` | re-export `AgentBacking`, `MAX_HUMAN_TEXT_BYTES` | Modify |
| `crates/orchestrator/src/executor/mod.rs` | `Fold.agent_answers`/`agent_prompts` + accessors; caller flag | Modify |
| `crates/orchestrator/src/executor/support.rs` | `fold_journal` arms | Modify |
| `crates/orchestrator/src/executor/human.rs` | **the human branch** | **Create** |
| `crates/orchestrator/src/executor/agent.rs` | the short-circuit between `assemble_prompt` and `resolve_chain` | Modify |
| `crates/torii/src/cmd/human.rs` | `run agent answer` | **Create** |
| `crates/torii/src/cmd/run.rs` | `signal_states` folds `AgentAwaited`; cross-refusal | Modify |
| `crates/torii/src/main.rs` | `AgentAction` + dispatch | Modify |
| `crates/torii/src/render.rs` | the question in the awaiting cell | Modify |
| `crates/torii/tests/e2e_pg.rs` | AC13 | Modify |

`human.rs` is a new file in each crate rather than more of `agent.rs`/`gate.rs`, matching how s2 put `run_human_gate` in its own `gate.rs`: `agent.rs` is the model path and stays that.

---

## Task 1: `AgentBacking` + the `validate()` rules

**Files:** Modify `crates/orchestrator-core/src/registry.rs`, `crates/orchestrator-core/src/lib.rs`. Test: same `registry.rs`, `mod tests`.

- [ ] **Step 1: Write the failing tests**

```rust
    fn human_agent(name: &str) -> AgentDefinition {
        AgentDefinition {
            name: name.to_string(),
            area: "review".into(),
            kind: "human".into(),
            chain: None,
            chains: HashMap::new(),
            grants: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "Does this contract permit sub-processing?".into(),
            backed_by: AgentBacking::Human { timeout: None },
        }
    }

    fn registry_of(agents: Vec<AgentDefinition>) -> Registry {
        let mut r = Registry::default();
        for a in agents {
            r.agents.insert(a.name.clone(), a);
        }
        r
    }

    /// A human-backed role has NO chain by construction, so `validate`'s
    /// chain-resolvability rule must not apply to it. That rule runs at config-LOAD
    /// time, independent of the executor's runtime short-circuit, so leaving it
    /// unconditional would reject essentially every human-backed agent before any
    /// node ever ran.
    #[test]
    fn a_human_backed_agent_needs_no_chain() {
        registry_of(vec![human_agent("reviewer")])
            .validate()
            .expect("a human-backed agent resolves no chain and must not need one");
    }

    /// A model-backed agent still does — the skip is narrow, not a hole.
    #[test]
    fn a_model_backed_agent_still_needs_a_chain() {
        let mut a = human_agent("modelled");
        a.backed_by = AgentBacking::Model;
        let e = registry_of(vec![a]).validate().expect_err("no chain, no binding");
        assert!(format!("{e}").contains("modelled"), "{e}");
    }

    /// Tools on a human-backed agent are never consulted — the loop that would use
    /// them never runs. A grant that grants nothing is the confused-deputy shape
    /// SP-4 s1 argues against, so reject the config rather than ignore it.
    #[test]
    fn a_human_backed_agent_may_not_declare_tools() {
        let mut a = human_agent("reviewer");
        a.tools = vec!["fs_read".into()];
        let e = registry_of(vec![a]).validate().expect_err("tools on a human agent");
        let m = format!("{e}");
        assert!(m.contains("reviewer"), "must name the agent: {m}");
        assert!(m.contains("tool"), "must name the rule: {m}");
    }

    /// The prompt IS the question. An empty one asks a human nothing.
    #[test]
    fn a_human_backed_agent_needs_a_system_prompt() {
        let mut a = human_agent("reviewer");
        a.system_prompt = String::new();
        let e = registry_of(vec![a]).validate().expect_err("empty prompt");
        assert!(format!("{e}").contains("reviewer"), "{e}");
    }

    /// `MAX_AWAIT_SIGNAL_TIMEOUT` bounds the sibling kinds in `Graph::validate_dag`,
    /// which is pure over the graph and never sees the registry — so the same bound
    /// has to be applied here instead.
    #[test]
    fn a_human_backed_timeout_obeys_the_century_bound() {
        let ok = |t| {
            let mut a = human_agent("reviewer");
            a.backed_by = AgentBacking::Human { timeout: Some(t) };
            registry_of(vec![a]).validate()
        };
        ok(chrono::Duration::hours(48)).expect("48h SLA");
        ok(crate::graph::MAX_AWAIT_SIGNAL_TIMEOUT).expect("exactly the bound");
        let e = ok(crate::graph::MAX_AWAIT_SIGNAL_TIMEOUT + chrono::Duration::days(1))
            .expect_err("over the bound");
        assert!(format!("{e}").contains("reviewer"), "{e}");
        let e = ok(chrono::Duration::zero()).expect_err("non-positive");
        assert!(format!("{e}").contains("reviewer"), "{e}");
    }
```

- [ ] **Step 2: Run and watch them fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_human_backed
```

Expected: **compile error** — `cannot find type AgentBacking` / no field `backed_by`.

- [ ] **Step 3: Add the type and the field**

In `crates/orchestrator-core/src/registry.rs`, above `AgentDefinition`:

```rust
/// What answers an agent: a model, or a person.
///
/// SP-6 s3. `Model` is the serde default, so every existing `AgentDefinition`,
/// every `config_agents` jsonb row and every registry fixture deserializes
/// unchanged — the same additivity discipline s1 and s2 used for their journal
/// events.
///
/// The timeout lives HERE rather than on `NodeKind::Agent` so the role and its SLA
/// travel together ("legal-reviewer always has 48h") and the graph never changes —
/// which is what makes a human-backed role substitutable at the `AgentRef`. The
/// cost, stated: one SLA per role, not per use site.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum AgentBacking {
    #[default]
    Model,
    Human {
        timeout: Option<chrono::Duration>,
    },
}
```

Add to `AgentDefinition`, after `system_prompt`:

```rust
    /// SP-6 s3: who answers. `#[serde(default)]` ⇒ absent means `Model`, so no
    /// existing config changes.
    #[serde(default)]
    pub backed_by: AgentBacking,
```

Re-export `AgentBacking` from `crates/orchestrator-core/src/lib.rs` alongside `AgentDefinition`.

- [ ] **Step 4: Make `validate()` skip the chain rule and add the three new ones**

Replace the chain check in `Registry::validate()` and add the rest:

```rust
            let human = matches!(agent.backed_by, AgentBacking::Human { .. });

            // SP-6 s3: a human-backed role resolves NO chain by construction, so the
            // chain-resolvability rule must not apply to it. This check runs at config
            // LOAD time, independent of `drive_agent`'s runtime short-circuit, so leaving
            // it unconditional would reject essentially every human-backed agent before
            // any node ever executed. Forcing an author to supply a dummy binding that is
            // never used would be a lie in the config.
            if !human
                && agent.chain.is_none()
                && self.chain_binding(&agent.area, &agent.kind).is_none()
            {
                return Err(OrchestratorError::UnknownChainRef {
                    agent: agent.name.clone(),
                });
            }

            if human {
                // The ReAct loop that would use these never runs, so a grant here grants
                // nothing — the confused-deputy shape SP-4 s1 argues against. Reject the
                // config rather than silently ignore the declaration.
                if !agent.tools.is_empty() {
                    return Err(OrchestratorError::InvalidConfig(format!(
                        "agent {:?} is human-backed and may not declare tools ({:?}); a \
                         human-backed agent answers once and never runs the tool loop",
                        agent.name, agent.tools
                    )));
                }
                // The prompt IS the question put to the human.
                if agent.system_prompt.trim().is_empty() {
                    return Err(OrchestratorError::InvalidConfig(format!(
                        "agent {:?} is human-backed and has an empty system_prompt; the \
                         prompt is the question, so an empty one asks the human nothing",
                        agent.name
                    )));
                }
                // `MAX_AWAIT_SIGNAL_TIMEOUT` bounds the sibling waiting kinds in
                // `Graph::validate_dag`, which is pure over the graph and never sees the
                // registry — so the same bound is applied here. Without it the overflow is
                // caught only at runtime by `wait_or_expire`'s `checked_add_signed` (which
                // fails the node rather than panicking, so it degrades safely), but both
                // sibling slices treated the up-front bound as worth naming.
                if let AgentBacking::Human { timeout: Some(t) } = &agent.backed_by {
                    if *t <= chrono::Duration::zero() {
                        return Err(OrchestratorError::InvalidConfig(format!(
                            "agent {:?} has a non-positive timeout ({t}); use `None` to \
                             wait indefinitely",
                            agent.name
                        )));
                    }
                    if *t > crate::graph::MAX_AWAIT_SIGNAL_TIMEOUT {
                        return Err(OrchestratorError::InvalidConfig(format!(
                            "agent {:?} has a timeout ({t}) that is too long; the maximum \
                             is {}; use `None` to wait indefinitely",
                            agent.name,
                            crate::graph::MAX_AWAIT_SIGNAL_TIMEOUT
                        )));
                    }
                }
            }
```

If `OrchestratorError::InvalidConfig` does not exist, use the nearest existing config-error variant rather than adding one, and say which you chose in your report.

- [ ] **Step 5: Run and watch them pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib a_human_backed
env -u DATABASE_URL cargo test --workspace
```

Every `AgentDefinition` literal in the workspace now needs `backed_by`. **Prefer `..Default::default()`** where the struct already supports it; otherwise add `backed_by: AgentBacking::Model`. Report how many fixtures you touched.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/registry.rs crates/orchestrator-core/src/lib.rs
git commit -F - <<'MSGEOF'
feat(core): SP-6 s3 (1/7) — AgentBacking + the four validate rules

Model is the serde default, so every existing AgentDefinition and every
config_agents jsonb row deserializes unchanged.

The load-bearing rule is the one REMOVED, not the ones added: validate()
unconditionally required every agent to resolve a chain, at config-LOAD time and
independent of any runtime short-circuit, so it would have rejected essentially
every human-backed agent before a node ever ran. A human-backed role has no chain
by construction. Caught by the spec's depth review, not by the build.

Three added: no tools (the loop that would use them never runs, so a grant here
grants nothing — the confused-deputy shape SP-4 s1 argues against), a non-empty
system_prompt (the prompt IS the question), and the MAX_AWAIT_SIGNAL_TIMEOUT
century bound — which lives in Graph::validate_dag for the sibling kinds, but
validate_dag is pure over the graph and never sees the registry.
MSGEOF
```

---

## Task 2: The two journal events + the shared bound

**Files:** Modify `crates/orchestrator-core/src/journal.rs`, `crates/orchestrator-core/src/lib.rs`. Test: `journal.rs`, `mod tests`.

- [ ] **Step 1: Write the failing test**

```rust
    /// SP-6 s3: both variants round-trip, and — the load-bearing half — they are new
    /// VARIANTS, so an event written by an older binary still loads and
    /// `FORMAT_VERSION` stays 1.
    #[test]
    fn the_human_agent_events_round_trip() {
        let awaited = JournalEvent::AgentAwaited {
            node: NodeId("review".into()),
            deadline: Some(chrono::DateTime::<chrono::Utc>::from_timestamp(3_000_000, 0).unwrap()),
            prompt: "Does this contract permit sub-processing?".into(),
        };
        let s = serde_json::to_string(&awaited).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::AgentAwaited { node, deadline, prompt } => {
                assert_eq!(node.0, "review");
                assert!(deadline.is_some());
                assert!(prompt.contains("sub-processing"));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let answered = JournalEvent::AgentAnswered {
            node: NodeId("review".into()),
            text: "Yes, clause 7.2 permits it.".into(),
            actor: "alice".into(),
        };
        let s = serde_json::to_string(&answered).expect("serializes");
        match serde_json::from_str::<JournalEvent>(&s).expect("round-trips") {
            JournalEvent::AgentAnswered { node, text, actor } => {
                assert_eq!(node.0, "review");
                assert!(text.contains("7.2"));
                assert_eq!(actor, "alice");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib the_human_agent_events_round_trip
```

Expected: **compile error** — `no variant named AgentAwaited`.

- [ ] **Step 3: Add the variants and the shared bound**

In `crates/orchestrator-core/src/journal.rs`, after the `GateDecided` variant:

```rust
    /// SP-6 s3: a human-backed `Agent` node has begun asking, carrying the QUESTION.
    ///
    /// The prompt is journaled rather than recomposed for the same reason s2 journals
    /// the menu: an operator must see what is being asked without reading the graph AND
    /// the registry, and fixing the question at ask time is what lets a late answer
    /// still be honoured against the question it was actually given.
    ///
    /// It is the full `assemble_prompt` output — system prompt + activated skills +
    /// the rendered context section — i.e. exactly what the model would have received.
    ///
    /// FIRST record wins when folded, exactly as `SignalAwaited`/`GateAwaited` do.
    AgentAwaited {
        node: NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: String,
    },
    /// SP-6 s3: a human answered a human-backed `Agent` node.
    ///
    /// `text` becomes the node's output under the `"text"` key — deliberately the same
    /// key a model-backed `Agent` produces, so `Consolidate`, `BranchCond::TextContains`
    /// and a dependent's prompt assembly consume it without knowing it was human.
    ///
    /// `actor` is ATTRIBUTION, NOT AUTHENTICATION, and it matters more here than on
    /// `GateDecided`: this string lands in the node's OUTPUT and flows into downstream
    /// model prompts, not merely an audit trail.
    AgentAnswered {
        node: NodeId,
        text: String,
        actor: String,
    },
```

Add to `crates/orchestrator-core/src/lib.rs` (or `journal.rs`, whichever holds shared consts):

```rust
/// The largest human-supplied answer, and the largest journaled question, in bytes.
///
/// SP-6 s3. It lives in `orchestrator-core` because BOTH crates need it and neither can
/// borrow the other's: `torii`'s `check_payload_size`/`MAX_PAYLOAD_BYTES` are
/// `pub(crate)`, and `orchestrator` does not depend on `torii` — that is a reverse
/// dependency the crate graph cannot express, not merely a visibility problem. One
/// constant, two call sites, no duplicated number.
///
/// 4 KiB matches `torii`'s `MAX_PAYLOAD_BYTES` and `split_output`'s `cas_threshold`. The
/// bound is load-bearing rather than theoretical for the PROMPT: an assembled prompt is
/// system prompt + every activated skill + the rendered context section, routinely
/// multi-KB.
pub const MAX_HUMAN_TEXT_BYTES: usize = 4096;
```

- [ ] **Step 4: Run and watch it pass**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator-core --lib the_human_agent_events_round_trip
env -u DATABASE_URL cargo test --workspace
```

A non-exhaustive `match` may break — add **explicit arms, never a catch-all `_ =>`**; this codebase has shipped fold bugs that way. The test-only `label()` helper in `executor/tests.rs` is the known one.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/journal.rs crates/orchestrator-core/src/lib.rs crates/orchestrator/src/executor/tests.rs
git commit -F - <<'MSGEOF'
feat(core): SP-6 s3 (2/7) — AgentAwaited/AgentAnswered + MAX_HUMAN_TEXT_BYTES

New variants, not new fields, so FORMAT_VERSION stays 1 — the additivity trick s1
and s2 both used.

AgentAwaited carries the QUESTION for the same reason s2 journals the menu: an
operator must see what is being asked without reading the graph AND the registry,
and fixing the question at ask time is what lets a late answer be honoured against
the question actually given.

MAX_HUMAN_TEXT_BYTES lives in orchestrator-core because both crates need it and
neither can borrow the other's: torii's check_payload_size is pub(crate), and
orchestrator does not depend on torii — a reverse dependency the crate graph
cannot express, not a visibility problem. The spec's first draft said "reuse
check_payload_size"; the depth review caught that it is impossible.
MSGEOF
```

---

## Task 3: Fold the two events

**Files:** Modify `crates/orchestrator/src/executor/mod.rs` (the `Fold` struct + accessors), `crates/orchestrator/src/executor/support.rs` (`fold_journal`). Test: `support.rs`, `mod tests`.

- [ ] **Step 1: Write the failing test**

```rust
    /// The two asymmetries are OPPOSITE and both load-bearing, exactly as s1's and s2's
    /// are: the ANSWER is last-wins (an operator corrects themselves before the run
    /// resumes) and the QUESTION is first-wins (the human was asked THIS question).
    #[test]
    fn agent_answers_are_last_wins_and_the_prompt_is_first_wins() {
        let events = vec![
            (
                1,
                JournalEvent::AgentAwaited {
                    node: NodeId("review".into()),
                    deadline: Some(at(1_000)),
                    prompt: "Original question?".into(),
                },
            ),
            (
                2,
                JournalEvent::AgentAnswered {
                    node: NodeId("review".into()),
                    text: "first answer".into(),
                    actor: "alice".into(),
                },
            ),
            (
                3,
                JournalEvent::AgentAnswered {
                    node: NodeId("review".into()),
                    text: "corrected answer".into(),
                    actor: "alice".into(),
                },
            ),
            (
                4,
                JournalEvent::AgentAwaited {
                    node: NodeId("review".into()),
                    deadline: Some(at(9_999)),
                    prompt: "Rewritten question?".into(),
                },
            ),
        ];
        let (fold, _, _) = fold_journal(&events);

        let a = fold
            .agent_answer_for(&NodeId("review".into()))
            .expect("answered");
        assert_eq!(a.text, "corrected answer", "LAST answer wins");
        assert_eq!(a.actor, "alice");

        assert_eq!(
            fold.prompt_for(&NodeId("review".into())),
            Some("Original question?"),
            "FIRST question wins — the human was asked THIS one"
        );
        assert_eq!(
            fold.deadline_for(&NodeId("review".into())),
            Some(Some(at(1_000))),
            "AgentAwaited folds into the SHARED deadlines map, first-wins"
        );
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib agent_answers_are_last_wins
```

Expected: **compile error** — `no method named agent_answer_for`.

- [ ] **Step 3: Add the fold state**

In `crates/orchestrator/src/executor/mod.rs`, near `GateDecision`:

```rust
/// SP-6 s3: a folded `AgentAnswered`.
#[derive(Debug, Clone, PartialEq)]
struct AgentAnswer {
    text: String,
    /// ATTRIBUTION, NOT AUTHENTICATION — see `JournalEvent::AgentAnswered`.
    actor: String,
}
```

Add to `struct Fold`, after `menus`:

```rust
    /// SP-6 s3: each human-backed agent node's answer, from `AgentAnswered`. LAST wins,
    /// like `signals`/`gate_decisions` and for the same reason: an operator must be able
    /// to correct an answer before the run resumes.
    agent_answers: HashMap<NodeId, AgentAnswer>,
    /// SP-6 s3: the QUESTION each human-backed agent node published when it began
    /// asking, from `AgentAwaited`. FIRST wins — the human was asked THIS question, and
    /// a later ask must not retroactively change what their answer was to.
    agent_prompts: HashMap<NodeId, String>,
```

Accessors in `impl Fold`, beside `menu_for`:

```rust
    /// SP-6 s3: the answer folded for this human-backed agent node.
    fn agent_answer_for(&self, node: &NodeId) -> Option<&AgentAnswer> {
        self.agent_answers.get(node)
    }

    /// SP-6 s3: the question this node published when it began asking. `None` = it has
    /// not asked yet — the trigger for `run_human_agent` to journal `AgentAwaited`
    /// FIRST, before reading any answer, so an answer without a question never arises.
    fn prompt_for(&self, node: &NodeId) -> Option<&str> {
        self.agent_prompts.get(node).map(String::as_str)
    }
```

In `crates/orchestrator/src/executor/support.rs`, add the arms beside the `GateAwaited` ones:

```rust
            // SP-6 s3: the ask. EXPLICIT, never folded by a catch-all — a catch-all
            // silently absorbing a new variant is how this codebase has shipped fold bugs.
            //
            // FIRST wins for BOTH the deadline and the prompt (`entry().or_insert`).
            // The deadline goes into the SHARED map because `wait_or_expire` reads
            // `deadline_for` and knows nothing about which kind recorded it.
            JournalEvent::AgentAwaited {
                node,
                deadline,
                prompt,
            } => {
                fold.deadlines.entry(node.clone()).or_insert(*deadline);
                fold.agent_prompts
                    .entry(node.clone())
                    .or_insert(prompt.clone());
            }
            // SP-6 s3: the answer. LAST wins (`insert` overwrites).
            JournalEvent::AgentAnswered { node, text, actor } => {
                fold.agent_answers.insert(
                    node.clone(),
                    AgentAnswer {
                        text: text.clone(),
                        actor: actor.clone(),
                    },
                );
            }
```

- [ ] **Step 4: Run, then mutation-verify**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib agent_answers_are_last_wins
env -u DATABASE_URL cargo test --workspace
```

Then in a **scratch copy** (`rsync -a --exclude=target --exclude=.git ./ /tmp/s3t3/`), confirm each mutation reddens the test on the assertion it names, and delete the scratch:

1. `agent_prompts.entry().or_insert()` → `.insert()` → red on "FIRST question wins".
2. `agent_answers.insert()` → `.entry().or_insert()` → red on "LAST answer wins".
3. Delete the whole `AgentAwaited` arm (letting the catch-all absorb it) → red on the deadline assertion.

Report all three.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs
git commit -F - <<'MSGEOF'
feat(orchestrator): SP-6 s3 (3/7) — fold the answer (last wins) and the question (first wins)

Two OPPOSITE asymmetries, mirroring s1's signals/deadlines and s2's
decisions/menus. The answer is last-wins so an operator can correct themselves
before the run resumes; the question is first-wins because the human was asked
THAT question and a later ask must not change what their answer was to.

AgentAwaited also folds into the SHARED deadlines map, because wait_or_expire
reads deadline_for and knows nothing about which kind recorded it.

Both arms EXPLICIT, never a catch-all — a catch-all silently absorbing a new
variant is how this codebase has shipped fold bugs before. Mutation-verified three
ways, each landing on the assertion it names.
MSGEOF
```

---

## Task 4: `run_human_agent` — the branch

**Files:** Create `crates/orchestrator/src/executor/human.rs`. Modify `crates/orchestrator/src/executor/mod.rs` (`mod human;`), `crates/orchestrator/src/executor/agent.rs` (the short-circuit). Test: `crates/orchestrator/src/executor/tests.rs`.

This task carries AC1, AC3, AC4, AC5, AC6, AC12, AC15, AC17.

- [ ] **Step 1: Write the failing tests**

Add a `mod human_agent` to `crates/orchestrator/src/executor/tests.rs`, modelled on `mod human_gate`. Reuse that module's `FakeClock`, `at()`, `recording_gateway()` and `paused_resume_afters` helpers rather than re-deriving them. The tests, each named for the AC it proves:

- `a_human_backed_agent_never_calls_the_gateway_and_still_answers` (AC1 + AC12) — drive; answer; drive again; assert `calls.lock().unwrap().len() == 0` **AND** `o.outputs[&review()]["text"] == "…"`. **The second assertion is the point:** s2 shipped an AC asserting only `calls == 0`, and since no gateway path was reachable it passed whether the node worked or was completely broken.
- `the_answer_is_the_nodes_output_under_the_text_key` (AC2) — a two-node graph, `review → route`, where `route` is a `Branch` with `BranchCond::TextContains("permits")`; assert the matching arm ran.
- `an_answer_inside_the_sla_is_honoured_by_a_late_drive` (AC3) — journal `AgentAwaited{deadline: t}` and `AgentAnswered`, set the clock past `t`, drive; assert `Completed`, NOT `Failed`. **This is the deliberate divergence from s2 and needs its own pin.**
- `a_fired_expiry_is_terminal_even_if_an_answer_arrives_later` (AC4) — expire first, then answer; assert it stays `Failed`.
- `an_expired_human_agent_never_produces_a_default_answer` (AC5) — expiry ⇒ `failed.is_some()` and `outputs` has no entry.
- `an_answer_delivered_before_the_node_first_runs_still_resolves` (AC6) — answer folded with no `AgentAwaited`; assert it resolves AND `AgentAwaited` is journaled.
- `a_timed_human_agent_pauses_on_the_absolute_deadline_it_recorded` — assert `RunPaused.resume_after == Some(recorded)` on the first drive and the SAME instant after a force-wake. s2's review found this untested and a `pause_awaiting(…, None)` mutation left the whole suite green.
- `the_journaled_prompt_is_the_assembled_prompt` (AC17) — a human-backed agent with an always-on skill; assert the journaled `prompt` contains the skill's body and the `## Context` section, not just `system_prompt`.
- `a_human_backed_agent_is_rejected_outside_a_top_level_agent_node` (AC15) — a human-backed `AgentRef` as a `MapBody::Agent` fails the node loudly, naming the site.

- [ ] **Step 2: Run and watch them fail**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib human_agent
```

Expected: compile error (no `run_human_agent`) or, once it compiles, every test red against the model path.

- [ ] **Step 3: Create `human.rs`**

```rust
//! The human-backed `Agent` node (SP-6 s3): a role answered by a person, not a model.
//!
//! s1 shipped `AwaitSignal` (pause, accept any JSON), s2 `HumanGate` (the typed menu).
//! This is the third and last waiting kind: an `Agent` node whose `AgentRef` resolves to
//! a human-backed definition pauses ONCE, journals the question it is asking, and
//! completes when a human answers.
//!
//! The waiting machinery is SHARED with both siblings, not copied — `gate_precheck` and
//! `wait_or_expire` live in `signal.rs`. s1's review found real defects in exactly those
//! arms; a third copy would be a third place for them to return.

use orchestrator_core::{JournalEvent, MAX_HUMAN_TEXT_BYTES, NodeId, OrchestratorError, RunId};

use super::signal::WaitState;
use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Execute one human-backed `Agent` node.
    ///
    /// | fold state | behaviour |
    /// |---|---|
    /// | failure recorded | `Failed` — shared `gate_precheck`, checked FIRST |
    /// | no question journaled yet | journal `AgentAwaited`, then continue below |
    /// | **answered** | `Completed({"text","actor"})` — **read BEFORE expiry** |
    /// | not answered, deadline passed | `NodeFailed` — the SLA fired with nobody answering |
    /// | not answered, deadline not passed | re-pause on the SAME absolute instant |
    ///
    /// **The answer is read BEFORE expiry, and that is a deliberate divergence from
    /// `HumanGate`.** s2 expires first because a gate decision is an APPROVAL and a late
    /// one must not approve a gate whose SLA ran out — the silent self-approval its §4
    /// rejects. An agent's answer is WORK PRODUCT, not an approval: there is nothing to
    /// self-approve, and discarding a human's in-time answer because a worker was down
    /// punishes them for infrastructure they had no part in. The deadline still fails the
    /// node in the case it exists for — nobody answered.
    ///
    /// **The ask precedes the answer, unconditionally**, for the reason s2 established:
    /// a durable question breaks s1's "the early race resolves itself for free" property,
    /// because an answer folded with no question has nothing to be an answer TO.
    ///
    /// No gateway call and no `EffectRecorded` — this function is reached before
    /// `resolve_chain`, so zero token spend is STRUCTURAL, not measured.
    pub(super) async fn run_human_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        prompt: &str,
        timeout: Option<chrono::Duration>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        if let Some(failed) = self.gate_precheck_by_id(node_id, fold) {
            return Ok(failed);
        }

        // The ask, first and unconditionally.
        let deadline = match self.wait_or_expire_by_id(node_id, timeout, fold) {
            Err(message) => {
                return self
                    .fail_human_agent(run, node_id, format!("human_agent: {message}"))
                    .await;
            }
            Ok(WaitState::NotYetAsking(fresh)) => {
                // Bound the QUESTION before it becomes durable. An assembled prompt is
                // system prompt + every activated skill + the rendered context section,
                // routinely multi-KB, so this is a real constraint. There is no exit-2
                // path inside the executor, so an over-bound prompt fails the node
                // loudly: a question too large to journal is a malformed agent config.
                if prompt.len() > MAX_HUMAN_TEXT_BYTES {
                    return self
                        .fail_human_agent(
                            run,
                            node_id,
                            format!(
                                "human_agent: node {}'s assembled prompt is {} bytes, over \
                                 the {MAX_HUMAN_TEXT_BYTES}-byte limit — trim the agent's \
                                 system prompt or its skills",
                                node_id.0,
                                prompt.len()
                            ),
                        )
                        .await;
                }
                self.append(
                    run,
                    JournalEvent::AgentAwaited {
                        node: node_id.clone(),
                        deadline: fresh,
                        prompt: prompt.to_string(),
                    },
                )
                .await?;
                fresh
            }
            Ok(WaitState::Expired(d)) => {
                return self
                    .fail_human_agent(
                        run,
                        node_id,
                        format!(
                            "human_agent: node {} passed its deadline {d} with no answer",
                            node_id.0
                        ),
                    )
                    .await;
            }
            Ok(WaitState::Waiting(d)) => d,
        };

        // Answered ⇒ complete, BEFORE any expiry consideration. Redact ONCE and hand
        // that one value to both the return and — via `apply_node_result` →
        // `publish_context` — the durable blackboard write. Splitting them makes a live
        // run and a replayed run disagree about this node's output, surfacing later as a
        // false `DeterminismViolation`; that defect has shipped and been caught twice
        // here.
        // The `{text, actor}` shape here is the one `project_agent_outputs`
        // (`executor/support.rs`) already passes through untouched — it exempts any
        // Agent output carrying an `actor`. Do NOT change these key names without
        // changing that exemption: the projection runs ONLY on the terminal-resume
        // path, so a mismatch is invisible on this drive and makes the finished run
        // report `{model: null, text}` when read back later (Task 2's review caught
        // this before the drive path existed; see the `AgentAnswered` doc).
        if let Some(answer) = fold.agent_answer_for(node_id) {
            let output = self.redact(&serde_json::json!({
                "text": answer.text,
                "actor": answer.actor,
            }));
            return Ok(NodeExec::Completed(output));
        }

        let reason = format!(
            "human_agent: waiting for a human answer on node {}{}",
            node_id.0,
            deadline
                .map(|d| format!(" (deadline {d})"))
                .unwrap_or_default()
        );
        self.pause_awaiting(run, reason, deadline).await
    }

    /// Journal a `NodeFailed` and return it. Every failure path above routes through
    /// here so the journaled message and the returned one cannot drift — and the
    /// message is redacted at this single chokepoint, because a prompt and an answer are
    /// both free text that reach the journal and `torii run status`. s2 shipped a
    /// per-arm scrub that missed one arm; a chokepoint makes that unrepresentable.
    async fn fail_human_agent(
        &self,
        run: RunId,
        node_id: &NodeId,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        let message = self.redact_text(message);
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node_id.clone(),
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

**`gate_precheck` and `wait_or_expire` currently take `&Node`, not `&NodeId`** — `drive_agent` has only a `&NodeId`. Add thin `_by_id` variants in `signal.rs` taking `&NodeId`, and make the existing `&Node` ones delegate to them, so there is still ONE implementation of each. Do not duplicate the bodies.

- [ ] **Step 4: Wire the short-circuit in `agent.rs`**

In `drive_agent`, between `assemble_prompt` (line ~75) and `resolve_chain` (line ~76):

```rust
        let (system, tools) = assemble_prompt(&self.registry, agent, context, &query)?;

        // SP-6 s3: a human-backed role answers instead of a model. The branch sits HERE —
        // after `assemble_prompt`, which needs no chain, so the human is shown exactly
        // what the model would have been; and BEFORE `resolve_chain`, so no chain is
        // resolved, no gateway is touched and zero token spend is structural rather than
        // measured.
        if let AgentBacking::Human { timeout } = agent.backed_by {
            if !top_level {
                // §5.5: legal ONLY as a top-level `NodeKind::Agent`. `drive_agent` is the
                // shared choke point for five callers, and the other four each mean a
                // different feature: N concurrent human asks for a Map, a human
                // re-answering every Loop iteration, a human deciding loop continuation,
                // and — worst — a human-backed planner, whose answer feeds
                // `parse_plan(text)`, so they would have to hand-author a machine-parseable
                // plan GRAPH. `validate_dag` cannot see the registry, so this is enforced
                // here at runtime rather than at load; that limitation is stated in §5.5.
                let message = format!(
                    "human_agent: agent {:?} is human-backed and may only be used as a \
                     top-level Agent node, not as a Map body, Loop body, Loop gate or \
                     planner",
                    agent_ref.0
                );
                self.append(run, JournalEvent::NodeFailed { node: node_id.clone(), error: message.clone() }).await?;
                return Ok(AgentStep::Failed(message));
            }
            return self
                .run_human_agent(run, node_id, &system, timeout, fold)
                .await
                .map(AgentStep::from_node_exec);
        }

        let chain = self.registry.resolve_chain(agent, phase)?.to_string();
```

Add a `top_level: bool` parameter to `drive_agent`; `run_node` passes `true`, the four other callers (`fanout.rs:183`, `:269`, `:488`, `:553`, `expand.rs:48`) pass `false`. **Read `AgentStep` before writing `from_node_exec`** — if it has no such conversion, write the mapping inline and say so.

**The `on_agent_started` hook does NOT fire on this path** — it requires `&ar.chain`, which does not exist before `resolve_chain`. That is correct and deliberate; note it in a comment.

- [ ] **Step 5: Run, then mutation-verify**

```bash
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib human_agent
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib await_signal   # must still be 15
env -u DATABASE_URL cargo test -p sensei-orchestrator --lib human_gate     # must still be 15
env -u DATABASE_URL cargo test --workspace
```

In a scratch copy, confirm each reddens:
1. Move the answer-read AFTER the expiry arm → `an_answer_inside_the_sla_is_honoured_by_a_late_drive` red (AC3 — the divergence).
2. Drop `gate_precheck_by_id` → `a_fired_expiry_is_terminal…` red (AC4).
3. `pause_awaiting(run, reason, None)` → the deadline test red.
4. Gate the ask on "unanswered" → the early-answer test red (AC6).
5. Ignore `top_level` → the AC15 test red.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/orchestrator/src/executor/human.rs crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/signal.rs crates/orchestrator/src/executor/tests.rs
git commit -F - <<'MSGEOF'
feat(orchestrator): SP-6 s3 (4/7) — run_human_agent, the third and last waiting kind

The branch sits between assemble_prompt and resolve_chain: after the prompt, so
the human sees exactly what the model would have; before the chain, so no gateway
is touched and zero token spend is STRUCTURAL rather than measured.

THE ANSWER IS READ BEFORE EXPIRY, and that is a deliberate divergence from
HumanGate. s2 expires first because a gate decision is an APPROVAL and a late one
must not approve a gate whose SLA ran out. An agent's answer is WORK PRODUCT:
there is nothing to self-approve, and discarding a human's in-time answer because
a worker was down punishes them for infrastructure. The deadline still fails the
node in the case it exists for — nobody answered.

Legal ONLY as a top-level Agent node. drive_agent is the shared choke point for
five callers and the other four are each a different feature; the planner case is
the sharpest, since the human's answer feeds parse_plan() and they would have to
hand-author a machine-parseable plan graph. validate_dag cannot see the registry,
so this is a runtime rejection, and that limitation is stated rather than hidden.

gate_precheck and wait_or_expire are SHARED with both siblings via thin _by_id
variants — one implementation, three callers. s1's review found real defects in
exactly those arms.

on_agent_started does NOT fire here: it needs a resolved chain that a human-backed
agent by construction never has.
MSGEOF
```

---

## Task 5: `torii run agent answer`

**Files:** Create `crates/torii/src/cmd/human.rs`. Modify `crates/torii/src/cmd/mod.rs`, `crates/torii/src/main.rs`, `crates/torii/src/cmd/run.rs`. Test: `cmd/human.rs` `mod tests`, `crates/torii/tests/cli.rs`.

Carries AC7, AC9, AC10, AC11.

- [ ] **Step 1: Write the failing tests**

Model `cmd/human.rs`'s `mod tests` on `cmd/gate.rs`'s, reusing `cmd::run::tests`'s `now`, `paused_store`, `FailingForceWakeStore` (already `pub(crate)`). Tests:

- `an_answer_to_a_node_that_never_asked_is_refused` — no `AgentAwaited` ⇒ `EXIT_PRECONDITION`, zero journaled rows.
- `an_answer_to_a_terminally_failed_node_is_refused` — the guard s2's review found missing on `gate decide`; assert the refusal names the state and journals nothing.
- `an_oversized_answer_is_rejected_before_anything_is_journaled` (AC10) — `--text` over `MAX_HUMAN_TEXT_BYTES`, measured **after** redaction; zero rows.
- `a_secret_shaped_answer_is_redacted_before_it_is_journaled` (AC9) — secret assembled at runtime (`format!("sk-{}", "A".repeat(24))`); assert `[REDACTED]` in the durable event and the secret absent.
- `a_legitimate_answer_is_journaled_and_queues_the_wake` — the positive guard, without which every refusal test above passes vacuously.
- `signal_on_a_human_backed_agent_is_refused_and_points_at_run_agent_answer` (AC7), plus the two symmetric refusals.

In `crates/torii/tests/cli.rs`:
- `run_help_lists_agent`
- `agent_answer_help_says_attribution_is_not_authentication`
- `an_answer_file_keeps_the_text_out_of_argv` (AC11) — spawn with `--text-file`, read the CHILD's own `ps` line, assert the sentinel is absent.

- [ ] **Step 2: Run and watch them fail**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib human::
```

Expected: compile error — `cannot find function answer`.

- [ ] **Step 3: Implement `cmd/human.rs`**

Read `crates/torii/src/cmd/gate.rs` in full first and mirror it — it already carries every lesson: append THEN `force_wake`; the seq-ordered post-write report distinguishing "a drive already in flight read it" from a true orphan; the `unread` closure that reports a durable-but-unqueued answer instead of `?`-ing a bare store error, with `render::safe_reason` applied to the backend message; `render::one_line` on every echoed string; and the node-state pre-check via `cmd::run::signal_state`.

**Validation is JOURNAL-ONLY** — fold `AgentAwaited` for the node and refuse if absent. Do **not** read the graph and do **not** consult the registry: `SchedulerStore::status()` returns a DTO documented "NOT the graph", no trait method exposes a run's graph, "is this human-backed" is a registry question torii cannot answer, and a path-qualified id has no `NodeKind` in the graph at all.

`--as` reuses `cmd::gate::actor_or`/`actor_or_user` — do not re-derive the `$USER` fallback.

`--text-file` content takes the **identical redact-then-cap ordering** as `--text`: read, redact, then check the size against the redacted value. That ordering shipped wrong twice in this feature.

- [ ] **Step 4: Wire clap and the cross-refusals**

Add `AgentAction::Answer { run_id, node, text: Option<String>, text_file: Option<PathBuf>, r#as: String }` under a `run agent` subcommand, with `text`/`text_file` in a required, mutually-exclusive `ArgGroup` — the shape `gate`'s `payload_src` group uses.

Add the third arm to the cross-refusal in `cmd::run::signal` and `cmd::gate::decide`, and the two symmetric refusals in `cmd::human::answer`. `cmd::run::signal_states` must fold `AgentAwaited` into `SignalState::Awaiting` (FIRST wins) or a human-backed node never appears in `list-paused` at all.

- [ ] **Step 5: Run and mutation-verify**

```bash
env -u DATABASE_URL cargo test -p sensei-torii
env -u DATABASE_URL cargo test --workspace
```

Scratch-copy mutations, each must redden:
1. Move the append before the size check → the oversized test's "zero rows" assertion.
2. Drop the `run signal` cross-refusal → its test.
3. Swap `append`/`force_wake` order → the ordering test.
4. Drop the node-state pre-check → the terminal-node test.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/human.rs crates/torii/src/cmd/mod.rs crates/torii/src/main.rs crates/torii/src/cmd/run.rs crates/torii/tests/cli.rs
git commit -F - <<'MSGEOF'
feat(torii): SP-6 s3 (5/7) — torii run agent answer

Validation is JOURNAL-ONLY, folding AgentAwaited. Not a simplification — the only
design that works: SchedulerStore::status() returns a DTO documented "NOT the
graph", no trait method exposes a run's graph, "is this human-backed" is a
registry question torii cannot answer, and a path-qualified id has no NodeKind in
the graph at all. The spec's first draft said "read the graph from scheduled_runs";
the depth review caught it, and s2's spec carries the same wrong sentence while its
implementation did the right thing.

--text-file ships NOW rather than as a follow-up. s1 added --payload argv-only and
a review caught the secret-in-ps exposure; s2 repeated it with --note. An agent's
answer is the longest free text of the three and the most likely to be pasted from
elsewhere, so shipping argv-only a third time knowing that would be a choice.

Follows cmd/gate.rs on every hard-won part: append THEN force_wake, the
seq-ordered post-write report, the post-append fault reported as a
durable-but-unqueued answer through safe_reason rather than a bare store error,
and the node-state pre-check s2's review found missing.
MSGEOF
```

---

## Task 6: `list-paused` shows the question

**Files:** Modify `crates/torii/src/cmd/run.rs` (`awaiting_nodes`), `crates/torii/src/render.rs`. Test: `cmd/run.rs` `mod tests`.

- [ ] **Step 1: Write the failing test**

`list_paused_shows_a_human_agents_question` — a run whose journal holds `AgentAwaited{prompt: "Does this contract permit sub-processing?"}`; assert the rendered table contains a truncated form of the question and the row reads `agent:`. **Anchor the assertion past the `AWAITING` header** — s2's equivalent test initially matched the REASON column, which incidentally contains the same text, and passed while the awaiting cell was empty.

Also `a_hostile_question_cannot_forge_an_awaiting_row_or_move_the_cursor` — a prompt containing `\n<uuid>  review  answered\u{1b}[2K`; assert no forged line and no surviving ESC.

- [ ] **Step 2: Run and watch it fail**

```bash
env -u DATABASE_URL cargo test -p sensei-torii --lib list_paused_shows_a_human_agents_question
```

- [ ] **Step 3: Implement**

Extend `render::AwaitingNode` with `question: Option<String>` (`#[serde(skip_serializing_if = "Option::is_none")]`, so s1/s2's `--json` stays byte-identical — the technique this command already uses for `awaiting_error` and `options`). Fold `AgentAwaited` in `awaiting_nodes`, FIRST wins. Render `agent: "…"` through `render::one_line` and `cap_chars` at a `QUESTION_MAX` — the question is unbounded-ish config text and reaches a line-oriented table.

- [ ] **Step 4: Run, mutation-verify, commit**

Mutations: drop the `AgentAwaited` fold in `signal_states` → the node vanishes from the listing entirely; drop `one_line` → the forged-row test reddens.

```bash
cargo fmt --all
git add crates/torii/src/cmd/run.rs crates/torii/src/render.rs
git commit -F - <<'MSGEOF'
feat(torii): SP-6 s3 (6/7) — list-paused shows the question

An operator cannot answer what they cannot see, and a human-backed agent's whole
point is that the question is the work. AgentAwaited carries it, so no graph load
is needed — which matters because list-paused folds one journal per paused run and
has no graph in hand.

The question is config text reaching a line-oriented table, so it goes through
one_line and a cap exactly as a node id and a gate menu do. The --json key is
skip_serializing_if so s1's and s2's output stays byte-identical.
MSGEOF
```

---

## Task 7: Cross-process e2e (AC13)

**Files:** Modify `crates/torii/tests/e2e_pg.rs`.

- [ ] **Step 1: Write the test**

Follow `a_human_gate_decided_in_another_process_completes_the_run` exactly. Shape: `n1 → review(human-backed Agent) → n2`. Process A pays for `n1`, the agent pauses durably with its question journaled. `run list-paused` on its OWN pool shows the question. `run agent answer` on a THIRD pool. A FRESH worker (store + journal + content + context + gateway, sharing nothing in-process) drives it through the real `worker serve --once` to `Completed`.

**Zero re-spend must be ATTRIBUTABLE, not an empty log** — filter the recording gateway's calls by PROMPT (which is the node id) and assert 1 for `n2` and **0 for `n1`**, matching s1/s2. Discrimination: swap the answer for a bare `wake` and the run must stay `Paused`.

Doc comment must say plainly: **`DATABASE_URL`-gated, returns early without one, therefore counted as PASSED while exercising nothing** — the raw-stderr `SKIP` is the only signal. Do not claim `#[ignore]`; it cannot be conditioned on an env var, and s2's spec was wrong about exactly this.

- [ ] **Step 2: Run it for real**

```bash
docker run -d --name s3-e2e-pg -e POSTGRES_PASSWORD=postgres -p 55434:5432 postgres:16
sleep 12
docker exec -i s3-e2e-pg psql -U postgres -v ON_ERROR_STOP=1 < database/_apply_all.sql
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55434/postgres \
  cargo test -p sensei-torii --test e2e_pg
echo "EXIT=$?"
docker rm -f s3-e2e-pg
```

**Read the real exit code.** Confirm **0 skips** and that `AgentAwaited`/`AgentAnswered` rows exist:

```bash
docker exec s3-e2e-pg psql -U postgres -t -A \
  -c "select count(*) from orchestrator.journal_events where event ? 'AgentAnswered'"
```

Remove **only** `s3-e2e-pg`. Other containers on this machine predate this work.

If you cannot run it, say so plainly and do NOT claim it passed. A skipped test reported as passing is the failure mode this project cares most about.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/torii/tests/e2e_pg.rs
git commit -F - <<'MSGEOF'
test(torii): SP-6 s3 (7/7) — AC13, the cross-process e2e

Process A pauses on a human-backed agent with its question journaled; list-paused
on its own pool shows the question; run agent answer delivers on a third; a FRESH
worker sharing nothing in-process drives it to Completed.

Zero re-spend is ATTRIBUTABLE rather than an empty log — calls are filtered by
prompt, which is the node id, and the same filter returns 1 for n2 and 0 for n1.
Discrimination proven: swap the answer for a bare wake and the run stays Paused.

DATABASE_URL-gated: it returns early without one and is therefore counted as
PASSED while having exercised nothing, with a raw-stderr SKIP as the only signal.
Stated because s2's spec claimed the test was #[ignore]d, which is impossible —
#[ignore] cannot be conditioned on an env var.
MSGEOF
```

---

## Final gate

Each command standalone; read its own exit code. No pipes.

- [ ] `env -u DATABASE_URL cargo test --workspace` → exit 0, **1505 + new**, 0 failed
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` → exit 0
- [ ] `cargo fmt --all --check` → exit 0
- [ ] `cargo doc --workspace --no-deps` → no NEW unresolved links (8 pre-existing in torii)
- [ ] The e2e against Docker Postgres → exit 0, **0 skips**
- [ ] Every AC1–AC17 has a named test observed to fail before its fix
- [ ] Run `/review-slice`, then re-review the fixes — the SP-6 s2 re-review found three HIGH defects introduced while fixing

## Spec coverage

| Spec § | Requirement | Task |
|---|---|---|
| §4 | `AgentBacking`, `backed_by` | 1 |
| §4 | The two events, `MAX_HUMAN_TEXT_BYTES` | 2 |
| §4 | Fold types + accessors, shared `deadlines` | 3 |
| §5.1–5.2 | The branch, the fold read, answer-before-expiry | 4 |
| §5.4 | `assemble_prompt` as the question | 4 |
| §5.5 | Top-level only; four sites rejected | 4 |
| §5.6 | `on_agent_started` does not fire | 4 |
| §5.7 | Redact once | 4 |
| §5.3 | Journal-only CLI validation, two layers | 4 (executor) + 5 (CLI) |
| §6 | Bounds, `--text-file`, `one_line`/cap | 4 (prompt) + 5 (text) + 6 (render) |
| §6 | `validate()`: skip chain, reject tools/empty prompt, timeout bound | 1 |
| §7 | Trust boundary in help text | 5 |
| §8 | AC1–AC6, AC12, AC15, AC17 | 4 |
| §8 | AC7, AC9, AC10, AC11 | 5 |
| §8 | AC2 (Branch composition) | 4 |
| §8 | AC13 | 7 |
| §8 | AC14, AC16 | Final gate, 1 |
