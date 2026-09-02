use serde::{Deserialize, Serialize};

use crate::error::OrchestratorError;
use crate::ids::NodeId;

/// The kind of work a node performs. Two variants: a raw `ModelCall` that
/// compiles directly into an `InferenceRequest` (slice 1), and an `Agent` node
/// that runs a durable ReAct loop over a named agent (slice 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeKind {
    ModelCall {
        chain: String,
        payload: serde_json::Value,
    },
    Agent {
        agent: crate::registry::AgentRef,
        input: serde_json::Value,
        /// Optional phase selecting a per-phase chain (`AgentDefinition::chains`);
        /// `None` resolves via the agent's explicit `chain` or its `(area,kind)`
        /// binding. A node attribute, fixed for the run — not a mid-loop transition.
        phase: Option<String>,
    },
    /// A single DAG node that fans out INTERNALLY over `over`, running `body`
    /// once per item concurrently (bounded by `concurrency`), then folding the
    /// children into one result under `aggregation` (§3.4). Graph-splicing the
    /// children as first-class nodes is deferred.
    Map {
        body: MapBody,
        over: Vec<serde_json::Value>,
        concurrency: usize,
        aggregation: Aggregation,
    },
    /// Aggregate the survivors of a `Map` (§3.5). Soft-depends on `over` (so it
    /// runs even when the Map ended `Failed`), reads the Map's **successful**
    /// results, and runs `body` once over them. If fewer than `min_viable`
    /// survived, it halts loudly (`ConsolidateStarved`) rather than synthesizing
    /// over an empty/degenerate set.
    Consolidate {
        over: NodeId,
        min_viable: usize,
        body: MapBody,
    },
    /// Iterate `body` at path `"{loop}/{i}"`, feeding each iteration's output into
    /// the next as input (refine), until `gate` says Stop or `max_iters` is
    /// reached. Cap-without-Stop completes best-effort (`converged: false`), never
    /// a bare fail (§10.3); a body failure fails the Loop. Output:
    /// `{ iterations, converged, output }`. The body drives a leaf effect
    /// (`ModelCall`/`Agent`) or a whole graph (`Subgraph`/`Expand`, SP-3 s5) per
    /// iteration; the gate is a pure predicate or a journaled gate-agent.
    Loop {
        body: LoopBody,
        input: serde_json::Value,
        gate: GateSpec,
        max_iters: usize,
    },
    /// A node whose work is a whole nested DAG, driven under this node's path in
    /// the SAME run (SP-3). `Box` breaks the recursive type (NodeKind → Graph →
    /// Node → NodeKind). Static this slice; slice 3 produces subgraphs at runtime.
    Subgraph { graph: Box<Graph> },
    /// A deterministic conditional (SP-3): test predecessor `on`'s output, run the
    /// first arm whose `BranchCond` matches (else `default`) as a nested graph under
    /// `"{branch}/{label}/…"`. Pure over `on`'s memoized output ⇒ resume recomputes
    /// the same arm, no branch journaling. Static this slice.
    Branch {
        on: NodeId,
        arms: Vec<(BranchCond, Graph)>,
        default: Graph,
    },
    /// A node that produces a nested subgraph AT RUNTIME (impure), drives it under
    /// `"{expand}/…"`, and folds its sink map as output (SP-3 slice 3). Unlike
    /// `Subgraph` (static) and `Branch` (pure decision), the produced graph comes
    /// from an injected `Planner`, so it is journaled as `PlanExpanded` and
    /// reconstructed from the journal on resume — never re-planned. `input` is a
    /// static `Value` this slice (author-provided); slice 4/5 threads it from a
    /// predecessor's output. No sibling-id references, so `namespace_graph`'s
    /// `other => other.clone()` arm and `validate_dag` need no `Expand` case.
    Expand {
        input: serde_json::Value,
        #[serde(default)]
        planner: PlannerRef,
    },
    /// SP-6 s1: pause until an external signal arrives for this node (HITL).
    ///
    /// `timeout` is a DURATION; the executor converts it to an absolute deadline ONCE,
    /// at first execution, and journals it (`SignalAwaited`). On the deadline with no
    /// signal the node FAILS — never a silent self-approval, which is why there is no
    /// default-payload option (spec §4).
    AwaitSignal { timeout: Option<chrono::Duration> },
    /// SP-6 s2: ask a human to pick one of an enumerated `options` menu — the TYPED
    /// layer over `AwaitSignal`, the way `Branch` layers a decision over an arbitrary
    /// predecessor output.
    ///
    /// Picking a [`GateOutcome::Complete`] option makes the decision this node's
    /// output — `{"decision", "actor", "note"}` — which `BranchCond::FieldEquals`
    /// matches directly, so a `HumanGate` composes with `Branch` unchanged. Picking a
    /// [`GateOutcome::Fail`] option journals `NodeFailed` and cascade-skips hard-edge
    /// dependents, exactly like any other node failure — there is no separate
    /// "rejected" status (see [`GateOutcome`]'s doc for the accepted cost of that).
    /// `timeout` has the SAME semantics as `AwaitSignal.timeout`: a DURATION the
    /// executor converts to an absolute deadline ONCE, at first execution, and on
    /// that deadline with no decision the node FAILS — never a silent default choice,
    /// for the same reason `AwaitSignal` has no default payload (spec §4). Answerable
    /// ONLY by a `GateDecided` naming one of `options`; an ordinary `SignalReceived` —
    /// the `AwaitSignal` answer — does NOT complete a gate, because it carries no menu
    /// choice to resolve against `Complete`/`Fail`.
    ///
    /// **`actor` is ATTRIBUTION, not AUTHENTICATION** (spec §7). It is whatever string the
    /// caller supplied (`torii run gate --as`, defaulting to `$USER`), so the output field
    /// records who CLAIMED to decide — anyone who can reach the journal can write any
    /// actor. It must NOT be branched on as an access control: nothing stops
    /// `BranchCond::FieldEquals("actor", "alice")`, and the `2b-quater` exhaustiveness
    /// check filters arms on `field == "decision"` only, so an actor-keyed arm validates
    /// silently and would give an author a two-person sign-off they do not have. Only
    /// `decision` is a decision.
    HumanGate {
        options: Vec<GateOption>,
        timeout: Option<chrono::Duration>,
    },
}

/// The largest timeout [`Graph::validate_dag`] accepts for [`NodeKind::AwaitSignal`] OR
/// [`NodeKind::HumanGate`] (SP-6 s2 gave the bound its second consumer — both compute
/// `now + timeout` through the same shared wait path): 100 Julian years.
///
/// It exists to bound `now + timeout`, which the executor computes on the node's first
/// execution. `chrono::Duration` spans ~±292 million years, but `DateTime<Utc>` ends at
/// +262143-12-31, so a sufficiently large duration makes that addition overflow — a
/// panic, and a durable one (see the `2b-bis` block in `validate_dag`).
///
/// Why a century, specifically:
///
/// * **It cannot overflow.** A machine's wall clock reads ~2026; even a clock skewed by
///   millennia leaves ~260,000 years of headroom above `now + 100y`. The check has to be
///   pure over the graph (there is no `now` at validation time), so a fixed bound with
///   six orders of magnitude of slack is the honest form of "addable to any plausible
///   `now`", and it is stable regardless of when the graph is validated versus run.
/// * **It costs nobody a real deadline.** The longest deadline this codebase accepts in
///   anger is `i32::MAX` seconds (~68 years) — already far past any human gate, and still
///   under this bound. "Wait longer than a century" is not a deadline; it is `None`, which
///   is the never-auto-woken class and the accurate way to say "no deadline".
pub const MAX_AWAIT_SIGNAL_TIMEOUT: chrono::Duration = chrono::Duration::days(36_525);

/// One choice a [`NodeKind::HumanGate`] offers, and what picking it does to the run.
///
/// Not to be confused with [`LoopGateOption`] (SP-6 s4) — both now put a NAMED MENU in
/// front of a human, so the two are easy to reach for interchangeably, but they answer
/// to different node kinds: this one belongs to the `HumanGate` node and its
/// [`GateOutcome`] decides that NODE's outcome (and a `Fail` option fails the run);
/// `LoopGateOption` belongs to a `Loop`'s `GateSpec::Human` and its `stops` decides
/// whether the LOOP keeps iterating — asked once per iteration, not once per run.
/// The tell is which field is on the option — `outcome` here, `stops` there.
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
/// **Accepted cost:** a `Fail` option and a dead provider both surface as the SAME
/// `RunStatus::Failed` — indistinguishable BY STATUS, distinguishable only by the reason
/// text `torii run status` renders. Neither one ever appears in `torii run list-paused`
/// (both are terminal, and that command filters on `status == Paused`); the cost falls
/// on anything else that filters on status alone — a script, or the terminal allowlist
/// `count_terminal_before`/`prune_terminal` use to decide what a retention sweep may
/// delete. A distinct `Rejected` status would be more truthful but reaches both store
/// impls, the dbd CHECK constraint and torii's rendering — deferred, not overlooked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateOutcome {
    /// The decision becomes this node's output; dependents run.
    Complete,
    /// `NodeFailed`; hard-edge dependents cascade-skip.
    Fail,
}

/// How an `Expand` node's plan is produced (SP-3 slice 4A). `Injected` = the
/// slice-3 `Planner` trait (deterministic/test); `Agent` = a journaled ReAct
/// planner agent (this slice). Slice 4B adds `Select` (goal-based selection).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum PlannerRef {
    Agent(crate::registry::AgentRef),
    #[default]
    Injected,
    /// Registry-driven: the executor's configured `PlannerSelector` picks a planner
    /// agent (from `area == PLANNER_AREA` candidates) for the goal (slice 4B).
    Select,
}

/// What a `Map`/`Consolidate` runs per item. A `ModelCall` child is one Pure
/// effect; an `Agent` child is a per-item ReAct sub-run (its effects nest under
/// the child path `"{node}/{i}"`), which is how the fan-out e2e drives real
/// agents through a reference chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MapBody {
    ModelCall { chain: String },
    Agent(crate::registry::AgentRef),
}

/// What a `Loop` runs per iteration (SP-3 s5). Leaf variants mirror `MapBody`; the
/// two graph variants drive a nested graph per iteration — `Subgraph` a static
/// author-provided graph (fresh re-run each iteration), `Expand` a planned graph
/// (plan+execute, the coordinator core).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoopBody {
    ModelCall { chain: String },
    Agent(crate::registry::AgentRef),
    Subgraph(Box<Graph>),
    Expand { planner: PlannerRef },
}

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
    /// menu that cannot converge.
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
/// on which node read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoopGateOption {
    /// What the operator types: `torii run gate decide … --option <name>`.
    pub name: String,
    /// `true` converges the loop; `false` runs another iteration (subject to `max_iters`).
    pub stops: bool,
}

/// A deterministic Stop condition for a [`NodeKind::Loop`], evaluated as a pure
/// function of one iteration's body output — so a resume recomputes the identical
/// decision from the memoized output, with no gate journaling (§10.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LoopGate {
    /// Stop when `output["text"]` contains this marker substring.
    TextContains(String),
    /// Stop when `output[field] == true` (strict JSON `true`).
    FieldTrue(String),
}

impl LoopGate {
    /// Whether this iteration's `output` satisfies the Stop condition.
    pub fn should_stop(&self, output: &serde_json::Value) -> bool {
        match self {
            LoopGate::TextContains(marker) => output
                .get("text")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.contains(marker.as_str())),
            LoopGate::FieldTrue(field) => output.get(field) == Some(&serde_json::Value::Bool(true)),
        }
    }
}

/// A pure predicate over a predecessor node's output, selecting a `Branch` arm
/// (mirrors `LoopGate`). Evaluated in arm order; first match wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BranchCond {
    /// `output[field] == value` (strict JSON equality) — switch on a discriminant.
    FieldEquals(String, serde_json::Value),
    /// `output[field] == true` (strict JSON `true`).
    FieldTrue(String),
    /// `output["text"]` contains this substring.
    TextContains(String),
}

impl BranchCond {
    /// Whether `output` satisfies this condition.
    pub fn matches(&self, output: &serde_json::Value) -> bool {
        match self {
            BranchCond::FieldEquals(f, v) => output.get(f) == Some(v),
            BranchCond::FieldTrue(f) => output.get(f) == Some(&serde_json::Value::Bool(true)),
            BranchCond::TextContains(s) => output
                .get("text")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.contains(s.as_str())),
        }
    }
}

/// How a `Map` folds its children's success/failure into the node's own status
/// (§3.4). The per-child manifest is always produced; `aggregation` only decides
/// whether the Map node itself is `Completed` or `Failed`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregation {
    /// The first child failure fails the Map (all children still recorded).
    FailFast,
    /// The Map always completes; failures live in the manifest.
    BestEffort,
    /// The Map completes iff the successful children clear the given
    /// threshold(s) — both must hold when both are set — else it fails (loud,
    /// manifest attached).
    Quorum {
        min_count: Option<usize>,
        min_fraction: Option<f64>,
    },
}

/// The strength of a dependency edge. A `Hard` edge means the dependent needs
/// its upstream to have *succeeded* (a failed/skipped upstream cascade-skips the
/// dependent); a `Soft` edge only needs the upstream to be *terminal* (completed
/// **or** failed/skipped), so a dependent still runs over whatever survived.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EdgeKind {
    Hard,
    Soft,
}

/// A typed dependency edge: the upstream node this one depends on, and how
/// strongly (see [`EdgeKind`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Dep {
    pub on: NodeId,
    pub kind: EdgeKind,
}

impl Dep {
    /// A hard dependency on `on` — cascade-skips this node if `on` fails/skips.
    pub fn hard(on: impl Into<NodeId>) -> Self {
        Self {
            on: on.into(),
            kind: EdgeKind::Hard,
        }
    }

    /// A soft dependency on `on` — this node still runs when `on` is terminal,
    /// whatever its outcome.
    pub fn soft(on: impl Into<NodeId>) -> Self {
        Self {
            on: on.into(),
            kind: EdgeKind::Soft,
        }
    }
}

/// A single node in the execution graph, with its explicit typed dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub kind: NodeKind,
    pub deps: Vec<Dep>,
}

/// An execution graph. Slice 1 validates that graphs are strictly linear.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    pub nodes: Vec<Node>,
}

impl Graph {
    /// Validate that the graph is strictly linear: node ids are distinct, the
    /// first node has no dependencies, and every subsequent node depends on
    /// exactly the immediately-prior node.
    pub fn validate_linear(&self) -> Result<(), OrchestratorError> {
        let mut seen = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen.insert(&node.id) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "duplicate node id: {:?}",
                    node.id
                )));
            }
        }
        for (i, node) in self.nodes.iter().enumerate() {
            if i == 0 {
                if !node.deps.is_empty() {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "first node {:?} must have no dependencies",
                        node.id
                    )));
                }
            } else {
                let prior = &self.nodes[i - 1].id;
                if node.deps.len() != 1 || &node.deps[0].on != prior {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "node {:?} must depend on exactly the prior node {:?}",
                        node.id, prior
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validate that the graph is a well-formed DAG: node ids are distinct,
    /// every `Dep.on` references a declared node, and the combined hard+soft
    /// dependency graph is acyclic (a topological order exists). A linear line
    /// is the trivial DAG that [`validate_linear`](Self::validate_linear) also
    /// accepts.
    pub fn validate_dag(&self) -> Result<(), OrchestratorError> {
        use std::collections::{HashMap, HashSet};

        // 1. Distinct node ids.
        let mut ids: HashSet<&NodeId> = HashSet::new();
        for node in &self.nodes {
            if !ids.insert(&node.id) {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "duplicate node id: {:?}",
                    node.id
                )));
            }
        }

        // 1b. SP-6 s1 (whole-slice review): `/` is the executor's node-PATH separator, and
        // it belongs to the executor, not to the author. Every nested construct namespaces
        // its inner nodes by `format!("{prefix}/{id}")` (`executor::subgraph::namespace_graph`),
        // and the runtime paths `{map}/{i}`, `{loop}/{i}`, `{expand}/__plan__` are built the
        // same way — so an author-supplied id containing `/` is an ALIAS for some nested
        // node's generated id.
        //
        // The reviewer's graph: `Subgraph("sg"){gate}`, whose inner node namespaces to
        // `"sg/gate"`, declared beside a top-level node literally named `sg/gate`. It
        // validated, and one `SignalReceived{node:"sg/gate"}` completed BOTH — a HITL
        // decision meant for one human gate silently answering another. The two ids are
        // distinct at THIS level (block 1 sees `sg` and `sg/gate`), so nothing here could
        // catch it after the fact; the collision only exists once nesting has flattened the
        // namespaces, by which point the fold is keyed and the damage is done.
        //
        // Rejecting the separator outright, rather than detecting post-namespacing
        // collisions, is the less disruptive of the two options the review offered: it is a
        // pure syntactic rule needing no cross-level analysis, it holds for runtime-produced
        // plans too (`plan::feasible` validates through this same function, so an untrusted
        // planner cannot emit an aliasing id either), and it costs nothing — no graph in
        // this workspace uses `/` in an author-supplied id, and `-`, `_`, `.` and `:` are
        // all still available. This checks only what the AUTHOR wrote: the executor's own
        // generated paths are namespaced AFTER validation and are never revalidated.
        for node in &self.nodes {
            if node.id.0.contains('/') {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "node id {:?} contains '/', which the executor reserves as the node-path \
                     separator for nested nodes (it would alias a namespaced node's id)",
                    node.id
                )));
            }
            for dep in &node.deps {
                if dep.on.0.contains('/') {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "node {:?} depends on {:?}, which contains the reserved '/' \
                         node-path separator",
                        node.id, dep.on
                    )));
                }
            }
        }

        // 2. Every dependency references a declared node.
        for node in &self.nodes {
            for dep in &node.deps {
                if !ids.contains(&dep.on) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "node {:?} depends on undeclared node {:?}",
                        node.id, dep.on
                    )));
                }
            }
        }

        // 2b. Per-node-kind sanity: a `Loop` needs at least one iteration — a
        // `max_iters == 0` Loop would complete degenerately with a null output, a
        // quiet degenerate path. Reject it loudly up front.
        for node in &self.nodes {
            if let NodeKind::Loop { max_iters: 0, .. } = &node.kind {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "loop node {:?} has max_iters == 0 (must be >= 1)",
                    node.id
                )));
            }
        }

        // 2b-bis. SP-6 s1: an `AwaitSignal`'s timeout is a DURATION, and
        // `chrono::Duration` happily represents zero and negatives (they even
        // round-trip through serde). `now + (-1h)` is a deadline already in the past,
        // so such a node would journal its deadline and immediately report a TIMEOUT —
        // a confusing failure for what is really a malformed graph. Same argument as
        // `max_iters == 0` above: reject the degenerate node loudly up front. `None`
        // (wait indefinitely) is the legitimate way to express "no deadline".
        //
        // The far end is worse than confusing. `chrono::Duration` runs to ~292 million
        // years but `DateTime<Utc>` stops at year 262143, so a large-enough timeout makes
        // the executor's `now + timeout` **panic**, and a panic here is durable: driven
        // through `Scheduler::submit` the store row is enqueued BEFORE the drive, so every
        // later `tick()` reclaims that row's stale lease and panics again — a poison pill
        // that takes the worker process down. Both callers hand this function
        // caller-controlled bytes (`torii run submit <graph.json>`, and an untrusted
        // `Expand` planner's plan via `plan::feasible`), so it is refused here, before
        // anything durable exists.
        for node in &self.nodes {
            let NodeKind::AwaitSignal {
                timeout: Some(timeout),
            } = &node.kind
            else {
                continue;
            };
            if *timeout <= chrono::Duration::zero() {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "await_signal node {:?} has a non-positive timeout ({timeout}); \
                     use `None` to wait indefinitely",
                    node.id
                )));
            }
            if *timeout > MAX_AWAIT_SIGNAL_TIMEOUT {
                return Err(OrchestratorError::InvalidGraph(format!(
                    "await_signal node {:?} has a timeout ({timeout}) beyond the \
                     {MAX_AWAIT_SIGNAL_TIMEOUT} maximum; use `None` to wait indefinitely",
                    node.id
                )));
            }
        }

        // 2b-ter. SP-6 s2: a `HumanGate`'s menu must be usable, and must offer a way
        // FORWARD. Same principle as `max_iters == 0` and the non-positive timeout
        // above: reject the degenerate node loudly here rather than let it produce a
        // baffling runtime state. A gate whose every option Fails is a guaranteed dead
        // end however the human answers — which is a malformed graph, not a policy.
        //
        // The timeout bounds are s1's, applied to this kind too: a `HumanGate` computes
        // `now + timeout` through the SAME shared wait path `AwaitSignal` does (Task 3
        // extracts it), so it is bound by the SAME two-layer defence against that
        // arithmetic leaving the representable `DateTime<Utc>` range (`chrono::Duration`
        // reaches ~292 million years; `DateTime<Utc>` stops at year 262143) — see
        // `signal.rs`'s `run_await_signal` step 2 for the full argument. This check is
        // layer 1: it refuses the graph up front, so a submit never enqueues a run whose
        // gate carries an unrepresentable deadline. Layer 2 lives where the addition
        // actually happens — `checked_add_signed`, not `+`. Layer 2 is NOT there because
        // validation might have been skipped: `run_inner` and `start_inner` both call
        // `validate_dag` before any node runs. It is there because validation and the
        // arithmetic are separate code, and a node kind must not panic on its own however
        // it was reached — a panic unwinds through `Scheduler::tick`, which has already
        // claimed a batch and taken its leases, so it takes the worker down and abandons
        // every other run in that batch. Defence in depth is cheap; a poisoned worker is
        // not.
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
            let mut seen = HashSet::new();
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
                        "human_gate node {:?} has a timeout ({t}) beyond the \
                         {MAX_AWAIT_SIGNAL_TIMEOUT} maximum; use `None` to wait indefinitely",
                        node.id
                    )));
                }
            }
        }

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
        //
        // This block, like 2b/2b-bis/2b-ter, only walks `self.nodes` at ONE level — it
        // does not itself recurse. A `Branch` and the `HumanGate` it switches on must
        // both be visible in the SAME `self.nodes` slice for this rule to fire: `on` is
        // a bare `NodeId`, not a path, so a gate nested one level down (inside a
        // `Subgraph`/`Loop` body) is invisible to a `Branch` at the outer level, and vice
        // versa — nothing in this codebase lets a `Branch` name a node outside its own
        // graph anyway (block 2's "undeclared node" check would already reject that
        // dependency). So a `Branch` and its `HumanGate` split across a nesting boundary
        // is not a hole this rule silently misses: it is a shape the rest of `validate_dag`
        // already forbids before this block would ever get the chance. Where both DO sit
        // together at some depth, block 2c's/2d's recursive `validate_dag()` calls run
        // this block again at that level, so the rule still fires wherever the pairing
        // exists.
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
            // The arms in DECLARATION order, kept alongside the set. Both are needed and
            // they are not interchangeable: the set answers membership, the vector decides
            // what the message SAYS. Reporting off a `HashSet` walk — which this block did
            // — made the arm named vary between processes on identical input, and the same
            // applies to the `options` recital below. That is not cosmetic: `feasible`
            // wraps this text as `PlanError::Structural`, `ValidatePlan` (a **Pure** tool)
            // returns it as its memoized output, and `drive_expand` journals it in a
            // `NodeFailed` — so a per-process ordering is a resume `DeterminismViolation`.
            // `feasible` itself sorts its errors for exactly this reason, and its sort key
            // is `format!("{a:?}")` OVER THESE STRINGS, so a varying message also reorders
            // the vector around it. Block 2b-ter already keeps its `HashSet` for
            // membership only (`seen`); this is the same discipline.
            let armed_in_order: Vec<&str> = arms
                .iter()
                .filter_map(|(cond, _)| match cond {
                    BranchCond::FieldEquals(field, value) if field == "decision" => value.as_str(),
                    _ => None,
                })
                .collect();
            let armed: HashSet<&str> = armed_in_order.iter().copied().collect();
            for o in options
                .iter()
                .filter(|o| o.outcome == GateOutcome::Complete)
            {
                if !armed.contains(o.name.as_str()) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch node {:?} switches on human_gate {:?} but has no arm for \
                         its Complete option {:?}; add an arm or the decision falls to \
                         `default` unnoticed",
                        node.id, on, o.name
                    )));
                }
            }
            let declared: HashSet<&str> = options.iter().map(|o| o.name.as_str()).collect();
            for a in &armed_in_order {
                if !declared.contains(a) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch node {:?} has an arm for {:?}, which human_gate {:?} does \
                         not declare; its options are: {}",
                        node.id,
                        a,
                        on,
                        options
                            .iter()
                            .map(|o| o.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
        }

        // 2c. A `Subgraph`'s nested graph must itself be a valid DAG (recursive).
        // A `Loop` with a `Subgraph` body has a static nested graph too — recurse
        // into it (a `LoopBody::Expand` has no static graph, so no recursion).
        for node in &self.nodes {
            if let NodeKind::Subgraph { graph } = &node.kind {
                graph.validate_dag()?;
            }
            if let NodeKind::Loop {
                body: LoopBody::Subgraph(graph),
                ..
            } = &node.kind
            {
                graph.validate_dag()?;
            }
        }

        // 2d. A `Branch`'s `on` must be a Hard dep of the branch (so it runs first
        // and a failed `on` cascade-skips the branch). This also enforces the
        // "declared" invariant: a Hard dep on an undeclared node is already rejected
        // by the general dependency check (block 2 above), so no separate
        // `ids.contains(on)` clause is needed. Each arm's and the default's nested
        // graph must itself be a valid DAG (recursive).
        for node in &self.nodes {
            if let NodeKind::Branch { on, arms, default } = &node.kind {
                if !node
                    .deps
                    .iter()
                    .any(|d| &d.on == on && matches!(d.kind, EdgeKind::Hard))
                {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch {:?} must Hard-depend on its `on` node {:?}",
                        node.id, on
                    )));
                }
                for (_, g) in arms {
                    g.validate_dag()?;
                }
                default.validate_dag()?;
            }
        }

        // 3. Acyclic — Kahn's algorithm. `in_degree` counts each node's deps
        // (edges point dep.on → node); repeatedly retire zero-in-degree nodes.
        // If any remain, a cycle exists (no topological order).
        let mut in_degree: HashMap<&NodeId, usize> =
            self.nodes.iter().map(|n| (&n.id, n.deps.len())).collect();
        let mut dependents: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();
        for node in &self.nodes {
            for dep in &node.deps {
                dependents.entry(&dep.on).or_default().push(&node.id);
            }
        }

        let mut ready: Vec<&NodeId> = in_degree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(id, _)| *id)
            .collect();
        let mut retired = 0usize;
        while let Some(id) = ready.pop() {
            retired += 1;
            if let Some(downstream) = dependents.get(id) {
                for dep_node in downstream {
                    let d = in_degree.get_mut(*dep_node).expect("declared node");
                    *d -= 1;
                    if *d == 0 {
                        ready.push(dep_node);
                    }
                }
            }
        }
        if retired != self.nodes.len() {
            return Err(OrchestratorError::InvalidGraph(
                "dependency cycle: no topological order exists".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **SP-6 s1 whole-slice review, Minor.** `/` is the executor's node-path separator,
    /// so an author-supplied id containing one aliases a nested node's generated id.
    ///
    /// The reviewer's graph: `Subgraph("sg"){gate}` — whose inner node is namespaced to
    /// `"sg/gate"` — beside a TOP-LEVEL node literally named `sg/gate`. It passed
    /// `validate_dag`, and one `SignalReceived{node:"sg/gate"}` completed BOTH: a HITL
    /// decision meant for one human gate silently answered another.
    ///
    /// Rejected at every level `validate_dag` recurses into, because the alias is created
    /// by nesting and the offender can sit at any depth.
    #[test]
    fn validate_dag_rejects_a_path_separator_in_an_author_supplied_node_id() {
        let offender = || Graph {
            nodes: vec![node("sg/gate", vec![])],
        };
        let assert_rejects = |g: &Graph, what: &str| match g.validate_dag() {
            Err(OrchestratorError::InvalidGraph(m)) => assert!(
                m.contains("sg/gate") && m.contains('/'),
                "{what}: rejected, but not for the id: {m}"
            ),
            other => panic!("{what}: expected InvalidGraph, got {other:?}"),
        };

        assert_rejects(&offender(), "top level");
        assert_rejects(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("s".into()),
                    kind: NodeKind::Subgraph {
                        graph: Box::new(offender()),
                    },
                    deps: vec![],
                }],
            },
            "nested in a Subgraph",
        );
        assert_rejects(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("L".into()),
                    kind: NodeKind::Loop {
                        body: LoopBody::Subgraph(Box::new(offender())),
                        input: serde_json::json!({}),
                        gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                        max_iters: 3,
                    },
                    deps: vec![],
                }],
            },
            "nested in a Loop body",
        );
        assert_rejects(
            &Graph {
                nodes: vec![
                    node("on", vec![]),
                    Node {
                        id: NodeId("b".into()),
                        kind: NodeKind::Branch {
                            on: NodeId("on".into()),
                            arms: vec![(BranchCond::FieldTrue("go".into()), offender())],
                            default: Graph { nodes: vec![] },
                        },
                        deps: vec![Dep::hard("on")],
                    },
                ],
            },
            "nested in a Branch arm",
        );

        // A dep may not reach INTO a nested namespace either. Such a graph is refused
        // either way — block 2 would call the id undeclared — so the assertion is on the
        // MESSAGE: an author who wrote `Dep::hard("sg/gate")` meant the subgraph's inner
        // node, and "undeclared node" sends them looking for a typo instead of telling them
        // the separator is reserved and cross-level edges do not exist.
        match (Graph {
            nodes: vec![node("a", vec![]), node("b", vec![Dep::hard("sg/gate")])],
        })
        .validate_dag()
        {
            Err(OrchestratorError::InvalidGraph(m)) => assert!(
                m.contains("sg/gate") && m.contains("separator"),
                "a dep into a namespace is refused for the SEPARATOR, not as a typo: {m}"
            ),
            other => panic!("expected InvalidGraph, got {other:?}"),
        }
    }

    /// The other half: ordinary ids — including the punctuation authors actually use —
    /// keep validating, so the rejection cannot have been written as a blanket refusal.
    #[test]
    fn validate_dag_accepts_ordinary_author_supplied_node_ids() {
        for id in [
            "gate",
            "n1",
            "review-legal",
            "review_legal",
            "gate.2",
            "gate:2",
            "gate-c3a9f0e4-1b2d-4c5e-8a7b-9d0e1f2a3b4c",
        ] {
            let g = Graph {
                nodes: vec![node(id, vec![])],
            };
            assert!(g.validate_dag().is_ok(), "{id:?} must still validate");
        }
    }

    #[test]
    fn validate_dag_rejects_a_zero_iteration_loop() {
        let graph = Graph {
            nodes: vec![Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: LoopBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({}),
                    gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                    max_iters: 0,
                },
                deps: vec![],
            }],
        };
        assert!(matches!(
            graph.validate_dag(),
            Err(OrchestratorError::InvalidGraph(_))
        ));
    }

    /// SP-6 s1: `chrono::Duration` permits negatives and zero, and both round-trip
    /// through serde perfectly — so a malformed graph reaches the executor, journals a
    /// deadline that is already in the past (or exactly `now`), and reports a TIMEOUT.
    /// That is a confusing failure for what is really an authoring mistake, so it is
    /// rejected loudly up front, exactly as `max_iters == 0` is.
    #[test]
    fn validate_dag_rejects_a_non_positive_await_signal_timeout() {
        for bad in [
            chrono::Duration::seconds(-3600),
            chrono::Duration::zero(),
            chrono::Duration::nanoseconds(-1),
        ] {
            let graph = Graph {
                nodes: vec![Node {
                    id: NodeId("gate".into()),
                    kind: NodeKind::AwaitSignal { timeout: Some(bad) },
                    deps: vec![],
                }],
            };
            let err = graph
                .validate_dag()
                .expect_err("a non-positive timeout is a degenerate node");
            assert!(
                matches!(err, OrchestratorError::InvalidGraph(_)),
                "expected InvalidGraph for {bad:?}, got {err:?}"
            );
        }
    }

    /// **SP-6 s1 whole-slice review, Critical.** The other end of the same guard.
    /// `chrono::Duration` reaches ~292 million years, but a `DateTime<Utc>` stops at
    /// year 262143 — so `now + timeout` does not merely produce a silly deadline, it
    /// **panics** (`DateTime + TimeDelta overflowed`) inside `run_await_signal`. Driven
    /// through `Scheduler::submit` the store row is enqueued BEFORE the drive, so the
    /// panic leaves a durable `(Waking, next_wake: None)` row that every later `tick()`
    /// reclaims and re-panics on: a poison pill that takes the worker down with it.
    ///
    /// This is the JSON the reviewer submitted, verbatim — `TimeDelta::MAX`, i.e.
    /// `i64::MAX` milliseconds — because both reachable callers hand `validate_dag`
    /// caller-controlled bytes: `torii run submit <graph.json>` and an `Expand`
    /// planner's emitted plan (`plan::feasible` validates through this same function).
    #[test]
    fn validate_dag_rejects_an_await_signal_timeout_that_cannot_be_added_to_now() {
        /// Rejected, and rejected FOR THE GATE — a nested case that merely errors (say,
        /// because the wrapper is malformed) would prove nothing about the recursion.
        fn assert_rejects_the_gate(graph: &Graph, what: &str) {
            match graph.validate_dag() {
                Err(OrchestratorError::InvalidGraph(m)) => assert!(
                    m.contains("await_signal node") && m.contains("gate"),
                    "{what}: rejected, but not for the gate's timeout: {m}"
                ),
                other => panic!("{what}: expected InvalidGraph, got {other:?}"),
            }
        }

        let json = r#"{"nodes":[{"id":"gate","kind":{"AwaitSignal":{"timeout":[9223372036854775,807000000]}},"deps":[]}]}"#;
        let graph: Graph = serde_json::from_str(json).expect("the reviewer's input parses");
        assert_rejects_the_gate(&graph, "the reviewer's graph");

        // `validate_dag` recurses into `Subgraph` and a `Loop`'s `Subgraph` body, so the
        // guard must reject a NESTED offender too — otherwise the poison pill just moves
        // one level down, which is exactly where a planner-emitted plan lands.
        assert_rejects_the_gate(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("S".into()),
                    kind: NodeKind::Subgraph {
                        graph: Box::new(graph.clone()),
                    },
                    deps: vec![],
                }],
            },
            "nested in a Subgraph",
        );
        assert_rejects_the_gate(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("L".into()),
                    kind: NodeKind::Loop {
                        body: LoopBody::Subgraph(Box::new(graph.clone())),
                        input: serde_json::json!({}),
                        gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                        max_iters: 3,
                    },
                    deps: vec![],
                }],
            },
            "nested in a Loop body",
        );
    }

    /// The other half of the guard above: a POSITIVE timeout and an ABSENT one (the
    /// indefinite HITL gate — the common case) must both still validate, so the
    /// rejection cannot have been written as a blanket refusal of `AwaitSignal`.
    ///
    /// The upper bound must not cost anyone a REAL deadline, so the accepted set spans
    /// from the smallest representable tick to `i32::MAX` seconds (~68 years) — longer
    /// than any human gate, and still comfortably under the bound.
    #[test]
    fn validate_dag_accepts_a_positive_or_absent_await_signal_timeout() {
        for ok in [
            Some(chrono::Duration::nanoseconds(1)),
            Some(chrono::Duration::seconds(3600)),
            Some(chrono::Duration::seconds(i64::from(i32::MAX))),
            None,
        ] {
            let graph = Graph {
                nodes: vec![Node {
                    id: NodeId("gate".into()),
                    kind: NodeKind::AwaitSignal { timeout: ok },
                    deps: vec![],
                }],
            };
            assert!(
                graph.validate_dag().is_ok(),
                "a well-formed AwaitSignal must validate: {ok:?}"
            );
        }
    }

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
        assert!(
            msg.contains("hold"),
            "must name the unhandled option: {msg}"
        );
        assert!(msg.contains("release"), "must name the gate: {msg}");

        // An arm naming an option the gate never declares — a typo, caught statically.
        let e = gate_then_branch(vec!["ship", "hold", "shipp"], three)
            .validate_dag()
            .expect_err("shipp is undeclared");
        assert!(format!("{e}").contains("shipp"), "{e}");
    }

    /// **This message must be byte-identical across processes**, because it does not stay
    /// a message: `feasible` wraps it as `PlanError::Structural(e.to_string())`, which
    /// `ValidatePlan::call` returns as its output — and `ValidatePlan` is a **Pure** tool,
    /// whose memoized output must be deterministic or a resume raises a
    /// `DeterminismViolation`. `Executor::drive_expand` journals the same text as
    /// `NodeFailed { error: "… infeasible plan: {errs:?}" }`. `feasible`'s own error sort
    /// (`errs.sort_by(format!("{a:?}"))`) is keyed on these strings too, so a varying
    /// message reorders the whole vector as well as itself.
    ///
    /// Both halves used to iterate `HashSet`s, and both varied. Measured across six
    /// consecutive processes: six different orderings of the recited menu, and — with two
    /// undeclared arms — the arm NAMED alternated between runs on identical input.
    /// `plan.rs` already sorts `feasible`'s errors for exactly this stated reason; this is
    /// the same rule one layer down.
    ///
    /// The two literals are pinned in DECLARATION order specifically because neither is
    /// alphabetical: `ship, hold, escalate, reject` sorts to `escalate, hold, reject,
    /// ship`, and `zzz_first` precedes `aaa_second` only in the order the author wrote the
    /// arms. A `HashSet` walk, or a sort, fails both.
    #[test]
    fn an_undeclared_arm_is_reported_deterministically_in_declaration_order() {
        let options = vec![
            opt("ship", GateOutcome::Complete),
            opt("hold", GateOutcome::Complete),
            opt("escalate", GateOutcome::Complete),
            opt("reject", GateOutcome::Fail),
        ];

        // (a) the recited MENU is the gate's `options`, in the order they are declared.
        let e = gate_then_branch(vec!["ship", "hold", "escalate", "typo"], options.clone())
            .validate_dag()
            .expect_err("`typo` is undeclared");
        let msg = format!("{e}");
        assert!(
            msg.contains("its options are: ship, hold, escalate, reject"),
            "the menu must be recited in DECLARATION order — this string is journaled and \
             memoized by a Pure tool, so a per-process ordering is a resume divergence: \
             {msg}"
        );

        // (b) with TWO undeclared arms, the one NAMED is the first in `arms` order.
        let e = gate_then_branch(
            vec!["ship", "hold", "escalate", "zzz_first", "aaa_second"],
            options,
        )
        .validate_dag()
        .expect_err("both extra arms are undeclared");
        let msg = format!("{e}");
        assert!(
            msg.contains("zzz_first"),
            "the FIRST offending arm in declaration order is the one reported: {msg}"
        );
        assert!(
            !msg.contains("aaa_second"),
            "…and only that one — reporting whichever the set happened to yield is what \
             made the arm named alternate between processes: {msg}"
        );
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

    /// A `Fail` option never produces an output for a `Branch` to switch on, so it needs
    /// no arm — only `Complete` options are exhaustiveness-checked. Guards the `filter(|o|
    /// o.outcome == GateOutcome::Complete)` above: widening that filter to cover `Fail`
    /// options too would ask the author to handle a value that cannot exist, and must
    /// turn this test RED.
    #[test]
    fn a_fail_option_needs_no_arm() {
        let opts = vec![
            opt("ship", GateOutcome::Complete),
            opt("reject", GateOutcome::Fail),
        ];
        gate_then_branch(vec!["ship"], opts)
            .validate_dag()
            .expect("Fail option `reject` needs no arm");
    }

    /// A gate must offer a real choice, and at least one way FORWARD. Same principle as
    /// `max_iters == 0` and a non-positive timeout: reject the degenerate node loudly at
    /// validation rather than let it produce a baffling runtime state.
    ///
    /// Every assertion also requires the error NAME THE NODE (`"release"`, the id
    /// `gate()` builds) — a stronger, reword-proof property than a phrase pin, and the
    /// same one every s1 sibling requires (`m.contains("gate")`, `m.contains("sg/gate")`).
    #[test]
    fn a_degenerate_gate_is_rejected() {
        // No options at all: nothing to pick.
        let e = gate(vec![], None)
            .validate_dag()
            .expect_err("empty options");
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(format!("{e}").contains("at least one option"), "{e}");

        // Every option fails: the run can NEVER proceed past this node, so the graph
        // is a guaranteed dead end however the human answers.
        let e = gate(
            vec![
                opt("reject", GateOutcome::Fail),
                opt("deny", GateOutcome::Fail),
            ],
            None,
        )
        .validate_dag()
        .expect_err("no Complete option");
        assert!(format!("{e}").contains("release"), "{e}");
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
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(format!("{e}").contains("duplicate"), "{e}");

        // An empty name cannot be typed at the CLI.
        let e = gate(vec![opt("", GateOutcome::Complete)], None)
            .validate_dag()
            .expect_err("empty name");
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(format!("{e}").contains("empty"), "{e}");
    }

    /// The timeout bounds are s1's, reused verbatim — a `HumanGate` computes `now +
    /// timeout` through the same shared code path, so it is bound by the same two-layer
    /// defence (see the `2b-ter` block's doc comment) against that arithmetic leaving the
    /// representable `DateTime<Utc>` range.
    ///
    /// Every assertion also requires the error NAME THE NODE (`"release"`), the same
    /// stronger property `a_degenerate_gate_is_rejected` pins.
    #[test]
    fn a_gate_timeout_obeys_the_same_bounds_as_await_signal() {
        let ok = vec![opt("approve", GateOutcome::Complete)];

        let e = gate(ok.clone(), Some(chrono::Duration::zero()))
            .validate_dag()
            .expect_err("zero timeout");
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(format!("{e}").contains("non-positive"), "{e}");

        let e = gate(ok.clone(), Some(chrono::Duration::hours(-1)))
            .validate_dag()
            .expect_err("negative timeout");
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(format!("{e}").contains("non-positive"), "{e}");

        let e = gate(
            ok.clone(),
            Some(MAX_AWAIT_SIGNAL_TIMEOUT + chrono::Duration::days(1)),
        )
        .validate_dag()
        .expect_err("over the century bound");
        // Not `contains("too long")`: that phrasing was dropped (Minor 4) to restore the
        // `use \`None\`` remedy s1's sibling message carries. Assert the node name and the
        // limit VALUE instead of a phrase, so a future reword of the sentence around them
        // cannot silently stop testing the thing that matters — which timeout, and which
        // node — without also breaking compilation of a stale phrase pin.
        assert!(format!("{e}").contains("release"), "{e}");
        assert!(
            format!("{e}").contains(&MAX_AWAIT_SIGNAL_TIMEOUT.to_string()),
            "{e}"
        );

        // The legitimate range still validates.
        gate(ok.clone(), None).validate_dag().expect("indefinite");
        gate(ok.clone(), Some(chrono::Duration::hours(48)))
            .validate_dag()
            .expect("48h SLA");
        gate(ok, Some(MAX_AWAIT_SIGNAL_TIMEOUT))
            .validate_dag()
            .expect("exactly the bound");
    }

    /// The 2b-ter block above only walks `self.nodes` at ONE level, same as 2b/2b-bis —
    /// it relies on block 2c's recursion (`graph.validate_dag()` on a `Subgraph`'s nested
    /// graph, and on a `Loop`'s `Subgraph` body) to reach a `HumanGate` buried inside one.
    /// Since that recursion calls the FULL `validate_dag` — not just the acyclic check —
    /// a degenerate nested `HumanGate` is caught the same way a degenerate nested
    /// `AwaitSignal` already is
    /// (`validate_dag_rejects_an_await_signal_timeout_that_cannot_be_added_to_now`, which
    /// this test mirrors for BOTH nesting shapes that sibling covers).
    #[test]
    fn validate_dag_recurses_into_a_nested_human_gate() {
        // Rejected, and rejected FOR THE GATE — a nested case that merely errors (say,
        // because the wrapper is malformed) would prove nothing about the recursion.
        fn assert_rejects_the_gate(graph: &Graph, what: &str) {
            match graph.validate_dag() {
                Err(OrchestratorError::InvalidGraph(m)) => assert!(
                    m.contains("release") && m.contains("at least one Complete"),
                    "{what}: rejected, but not for the nested gate's missing Complete \
                     option: {m}"
                ),
                other => panic!("{what}: expected InvalidGraph, got {other:?}"),
            }
        }

        let degenerate = || gate(vec![opt("reject", GateOutcome::Fail)], None);

        assert_rejects_the_gate(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("s".into()),
                    kind: NodeKind::Subgraph {
                        graph: Box::new(degenerate()),
                    },
                    deps: vec![],
                }],
            },
            "nested in a Subgraph",
        );
        assert_rejects_the_gate(
            &Graph {
                nodes: vec![Node {
                    id: NodeId("L".into()),
                    kind: NodeKind::Loop {
                        body: LoopBody::Subgraph(Box::new(degenerate())),
                        input: serde_json::json!({}),
                        gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                        max_iters: 3,
                    },
                    deps: vec![],
                }],
            },
            "nested in a Loop body",
        );
    }

    #[test]
    fn branch_cond_matches_each_variant() {
        let out = serde_json::json!({ "status": "b", "done": true, "text": "hello world" });
        assert!(BranchCond::FieldEquals("status".into(), serde_json::json!("b")).matches(&out));
        assert!(!BranchCond::FieldEquals("status".into(), serde_json::json!("a")).matches(&out));
        assert!(BranchCond::FieldTrue("done".into()).matches(&out));
        assert!(!BranchCond::FieldTrue("missing".into()).matches(&out));
        assert!(!BranchCond::FieldTrue("status".into()).matches(&out)); // "b" is not `true`
        assert!(BranchCond::TextContains("world".into()).matches(&out));
        assert!(!BranchCond::TextContains("zzz".into()).matches(&out));
        // TextContains only inspects `text`.
        assert!(
            !BranchCond::TextContains("b".into()).matches(&serde_json::json!({ "status": "b" }))
        );
    }

    #[test]
    fn loop_gate_should_stop_is_pure_over_output() {
        let text = LoopGate::TextContains("DONE".into());
        assert!(text.should_stop(&serde_json::json!({ "text": "all DONE here" })));
        assert!(!text.should_stop(&serde_json::json!({ "text": "keep going" })));
        assert!(!text.should_stop(&serde_json::json!({ "other": "DONE" }))); // only checks `text`
        let field = LoopGate::FieldTrue("done".into());
        assert!(field.should_stop(&serde_json::json!({ "done": true })));
        assert!(!field.should_stop(&serde_json::json!({ "done": false })));
        assert!(!field.should_stop(&serde_json::json!({ "done": "true" }))); // strict: JSON true only
        assert!(!field.should_stop(&serde_json::json!({})));
    }

    /// AC1 — the new variant round-trips through serde.
    #[test]
    fn a_human_gate_spec_round_trips_through_serde() {
        let gate = GateSpec::Human {
            agent: crate::registry::AgentRef("reviewer".into()),
            menu: vec![
                LoopGateOption {
                    name: "keep-going".into(),
                    stops: false,
                },
                LoopGateOption {
                    name: "good-enough".into(),
                    stops: true,
                },
            ],
        };
        let json = serde_json::to_string(&gate).expect("serialises");
        // Pin the wire bytes, not just the round-trip: a round-trip alone is invariant
        // under `#[serde(rename = "halts")] pub stops: bool` (renames both directions
        // together). The mutation with teeth is that rename PLUS `#[serde(default)]` —
        // a missing `stops`/`halts` field then silently reads as `false`, flipping every
        // stopping option in an already-persisted `scheduled_runs.graph` row to
        // non-stopping, which is the exact failure this variant exists to prevent. This
        // assertion is what would catch it; it is not redundant with the round-trip above.
        assert_eq!(
            json,
            r#"{"Human":{"agent":"reviewer","menu":[{"name":"keep-going","stops":false},{"name":"good-enough","stops":true}]}}"#
        );
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
    /// today. Guards against a change to the TAGGING REPRESENTATION — `#[serde(tag =
    /// …)]`, `untagged`, a rename — silently rewriting every existing
    /// `scheduled_runs.graph` row.
    ///
    /// It does NOT catch variant REORDERING, and that is not a gap here: externally
    /// tagged serde (today's default, unchanged by this variant) keys JSON by variant
    /// NAME, so order cannot affect the output or name-matched deserialisation. Order
    /// would only matter under `untagged` or an index-based binary format, and this
    /// workspace persists `Graph` as JSON/jsonb everywhere (no `bincode`/`postcard`/
    /// `rmp_serde`/`ciborium`/`serde_cbor` in any crate). If one is ever added, this
    /// test stops being sufficient and a round-trip through THAT format is what closes
    /// the gap.
    #[test]
    fn an_existing_pure_gate_serialises_unchanged_by_the_new_variant() {
        let gate = GateSpec::Pure(LoopGate::TextContains("DONE".into()));
        assert_eq!(
            serde_json::to_string(&gate).expect("serialises"),
            r#"{"Pure":{"TextContains":"DONE"}}"#
        );
    }

    /// A minimal node carrying a throwaway `ModelCall` kind — `validate_dag`
    /// inspects only ids + deps, never the kind.
    fn node(id: &str, deps: Vec<Dep>) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::ModelCall {
                chain: "c".into(),
                payload: serde_json::json!({}),
            },
            deps,
        }
    }

    #[test]
    fn validate_dag_accepts_a_linear_line() {
        // a → b → c: a line is a valid DAG.
        let g = Graph {
            nodes: vec![
                node("a", vec![]),
                node("b", vec![Dep::hard("a")]),
                node("c", vec![Dep::hard("b")]),
            ],
        };
        assert!(g.validate_dag().is_ok());
    }

    #[test]
    fn validate_dag_accepts_a_diamond_with_mixed_edges() {
        // a → {b, c} → d, where d soft-depends on c and hard-depends on b.
        let g = Graph {
            nodes: vec![
                node("a", vec![]),
                node("b", vec![Dep::hard("a")]),
                node("c", vec![Dep::hard("a")]),
                node("d", vec![Dep::hard("b"), Dep::soft("c")]),
            ],
        };
        assert!(g.validate_dag().is_ok(), "a diamond is acyclic");
    }

    #[test]
    fn validate_dag_rejects_a_cycle() {
        // a → b → a: no topological order exists.
        let g = Graph {
            nodes: vec![
                node("a", vec![Dep::hard("b")]),
                node("b", vec![Dep::hard("a")]),
            ],
        };
        let err = g.validate_dag().expect_err("a cycle has no topo order");
        assert!(matches!(err, OrchestratorError::InvalidGraph(_)), "{err:?}");
    }

    #[test]
    fn validate_dag_rejects_a_dep_on_an_undeclared_node() {
        let g = Graph {
            nodes: vec![node("a", vec![Dep::hard("ghost")])],
        };
        let err = g
            .validate_dag()
            .expect_err("a dep must reference a declared node");
        assert!(matches!(err, OrchestratorError::InvalidGraph(_)), "{err:?}");
    }

    #[test]
    fn validate_dag_rejects_duplicate_node_ids() {
        let g = Graph {
            nodes: vec![node("a", vec![]), node("a", vec![])],
        };
        let err = g.validate_dag().expect_err("node ids must be distinct");
        assert!(matches!(err, OrchestratorError::InvalidGraph(_)), "{err:?}");
    }

    #[test]
    fn validate_dag_accepts_a_soft_self_free_multi_root() {
        // Two independent roots feeding one sink — still a DAG.
        let g = Graph {
            nodes: vec![
                node("a", vec![]),
                node("b", vec![]),
                node("sink", vec![Dep::soft("a"), Dep::soft("b")]),
            ],
        };
        assert!(g.validate_dag().is_ok());
    }

    #[test]
    fn validate_dag_recurses_into_subgraphs() {
        let nested_cycle = Graph {
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
        };
        let outer = Graph {
            nodes: vec![Node {
                id: NodeId("s".into()),
                kind: NodeKind::Subgraph {
                    graph: Box::new(nested_cycle),
                },
                deps: vec![],
            }],
        };
        assert!(
            matches!(
                outer.validate_dag(),
                Err(OrchestratorError::InvalidGraph(_))
            ),
            "a nested cycle is rejected recursively"
        );

        let nested_ok = Graph {
            nodes: vec![
                Node {
                    id: NodeId("a".into()),
                    kind: NodeKind::ModelCall {
                        chain: "c".into(),
                        payload: serde_json::json!(0),
                    },
                    deps: vec![],
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
        };
        let outer_ok = Graph {
            nodes: vec![Node {
                id: NodeId("s".into()),
                kind: NodeKind::Subgraph {
                    graph: Box::new(nested_ok),
                },
                deps: vec![],
            }],
        };
        assert!(outer_ok.validate_dag().is_ok());
    }

    #[test]
    fn validate_dag_rejects_a_cycle_in_a_loop_subgraph_body() {
        // A `Loop` with a `Subgraph` body carries a static nested graph; a cycle
        // inside it must be rejected recursively (SP-3 s5), mirroring the top-level
        // `Subgraph` recursion in `validate_dag_recurses_into_subgraphs`.
        let nested_cycle = Graph {
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
        };
        let outer = Graph {
            nodes: vec![Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: LoopBody::Subgraph(Box::new(nested_cycle)),
                    input: serde_json::json!({}),
                    gate: GateSpec::Pure(LoopGate::TextContains("x".into())),
                    max_iters: 1,
                },
                deps: vec![],
            }],
        };
        assert!(
            matches!(
                outer.validate_dag(),
                Err(OrchestratorError::InvalidGraph(_))
            ),
            "a cycle in a Loop's Subgraph body is rejected recursively"
        );
    }

    #[test]
    fn expand_deserializes_without_planner_as_injected() {
        let j = r#"{"Expand":{"input":{}}}"#;
        let k: NodeKind = serde_json::from_str(j).unwrap();
        assert!(matches!(
            k,
            NodeKind::Expand {
                planner: PlannerRef::Injected,
                ..
            }
        ));
    }

    #[test]
    fn planner_ref_agent_roundtrips() {
        let r = PlannerRef::Agent(crate::registry::AgentRef("planner".into()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<PlannerRef>(&s).unwrap(), r);
    }

    /// SP-6 s1: confirms (by actually round-tripping, not just compiling) that
    /// `chrono::Duration` serializes/deserializes under `serde_json` given this crate's
    /// `chrono = { features = ["serde"] }` — the fact `AwaitSignal.timeout` relies on.
    #[test]
    fn await_signal_timeout_roundtrips_as_a_chrono_duration() {
        let k = NodeKind::AwaitSignal {
            timeout: Some(chrono::Duration::seconds(3600)),
        };
        let s = serde_json::to_string(&k).unwrap();
        let back: NodeKind = serde_json::from_str(&s).unwrap();
        match back {
            NodeKind::AwaitSignal { timeout } => {
                assert_eq!(timeout, Some(chrono::Duration::seconds(3600)));
            }
            other => panic!("expected AwaitSignal, got {other:?}"),
        }

        let none = NodeKind::AwaitSignal { timeout: None };
        let s = serde_json::to_string(&none).unwrap();
        assert!(matches!(
            serde_json::from_str::<NodeKind>(&s).unwrap(),
            NodeKind::AwaitSignal { timeout: None }
        ));
    }

    /// Every node kind an author can write must be DOCUMENTED on the feature doc that
    /// promises to enumerate them — not merely mentioned somewhere in its prose.
    ///
    /// `execution-graph.md`'s status paragraph opens "Implemented node kinds: …", and every
    /// node-kind slice before this one updated it (`Subgraph`, `Branch`, `Loop` bodies).
    /// SP-6 s1 edited the module README row that LINKS to that page — marking the feature
    /// "SP-1/3 · SP-6-1" with `AwaitSignal` — while leaving the page itself at eight of
    /// nine, so the two surfaces contradicted each other and a graph author following the
    /// link concluded HITL was not available.
    ///
    /// Asserted against the enum rather than against a hand-kept list, so the next node
    /// kind cannot ship undocumented either.
    ///
    /// **The first version of this guard did not work, and its failure is the reason the
    /// rule below is shaped the way it is.** It was `doc.contains(variant)` — a bare
    /// substring search over the whole 161-line file — which cannot tell "this kind is
    /// documented" from "this name occurs in a sentence". `HumanGate` was named twice
    /// before it had a single line of documentation: once as a forward reference in the
    /// BODY of the `AwaitSignal` bullet ("`HumanGate` (s2) and human-as-Agent (s3) are
    /// typed wrappers over it"), and once in the aspirational "Node kinds: …" sentence
    /// below the blockquote — a sentence that also names `Tool`, which has never been a
    /// variant at all. So the guard was GREEN across the whole commit that introduced the
    /// variant, and the RED the slice plan predicted never happened. A guard that passes
    /// while the thing it guards is absent is worse than no guard, because it is believed.
    ///
    /// So this asks for the two shapes the page actually uses to document a kind, and it
    /// asks for them **cumulatively**:
    ///
    /// * **Rule 1, every variant** — a backticked name in the "Implemented node kinds:"
    ///   paragraph, bounded to that one markdown paragraph. That paragraph is a CLOSED
    ///   enumeration: it opens by promising to list the implemented kinds, so a kind it
    ///   omits is a kind the page states does not exist.
    /// * **Rule 2, additionally, every kind added since `Subgraph`** — its own bullet, a
    ///   line whose HEAD is ``> - **`Name …`**``. Matching the head, not the line, is what
    ///   makes the forward reference inside another kind's bullet body insufficient. The
    ///   five kinds in `GRANDFATHERED` predate the bullet convention and have no bullet to
    ///   find, so rule 1 alone documents them.
    ///
    /// **The two rules were ALTERNATIVES until SP-6 s2's review, and that is how the
    /// enumeration went stale.** `HumanGate` shipped with a full bullet and satisfied the
    /// `has_own_bullet || (grandfathered && in the enumeration)` test, so the guard stayed
    /// green while the sentence a reader hits FIRST — "Implemented node kinds: …" — still
    /// listed nine of ten and still carried the marker `SP-6-1`. An author reading it top
    /// to bottom concluded the kind did not exist, which is the exact failure the guard was
    /// written for, one paragraph away from where it was found.
    ///
    /// Neither shape is reachable from prose, which is the whole point: `Tool` is the
    /// standing proof that a mention on this page is not a promise about the code.
    #[test]
    fn every_node_kind_is_documented_in_the_execution_graph_feature_doc() {
        // The variant names, read off the source of truth rather than restated.
        let src = include_str!("graph.rs");
        let decl = src
            .split_once("pub enum NodeKind {")
            .expect("the enum is declared here")
            .1;
        let body = decl.split_once("\n}\n").expect("the enum ends").0;
        let variants: Vec<&str> = body
            .lines()
            .filter_map(|l| {
                let t = l.trim_start();
                // A variant line is four-space-indented and starts a `Name {` or `Name(`.
                if l.starts_with("    ") && !l.starts_with("     ") && !t.starts_with("//") {
                    t.split(|c: char| !c.is_alphanumeric())
                        .next()
                        .filter(|n| n.chars().next().is_some_and(char::is_uppercase))
                } else {
                    None
                }
            })
            .collect();
        assert!(
            variants.len() >= 10,
            "the variant scrape broke — found {variants:?}"
        );

        let doc = include_str!("../../../docs/features/orchestrator/execution-graph.md");

        // The kinds that predate the per-kind bullet convention: they are documented by
        // the status enumeration alone, and retro-fitting five bullets is not this test's
        // job. FROZEN — adding a name here to make this test pass is exactly the move the
        // test exists to catch, so a new kind gets a bullet instead. Every entry is checked
        // against the scrape below, so a renamed variant cannot leave a dead free pass.
        const GRANDFATHERED: [&str; 5] = ["ModelCall", "Agent", "Map", "Consolidate", "Loop"];
        for legacy in GRANDFATHERED {
            assert!(
                variants.contains(&legacy),
                "`{legacy}` is grandfathered out of the bullet requirement but is no longer \
                 a `NodeKind` variant — remove it here rather than leave an entry that \
                 exempts nothing"
            );
        }

        // The "Implemented node kinds:" enumeration, bounded to its OWN markdown paragraph
        // (up to the first blank blockquote line). Deliberately not the whole status
        // blockquote: the prose after that break names most of the kinds again while
        // documenting none of them, and reading it would re-open the hole above.
        let status = doc
            .split_once("Implemented node kinds:")
            .expect("the feature doc still opens with its node-kind enumeration")
            .1
            .split("\n>\n")
            .next()
            .expect("the enumeration paragraph ends");

        // `> - **`Name` …` at the HEAD of a line. The `> ` is stripped so a bullet that
        // later moves out of the blockquote still counts, and the character after the name
        // must be non-alphanumeric so a `HumanGateV2` bullet cannot document `HumanGate`.
        let has_own_bullet = |v: &str| {
            doc.lines().any(|l| {
                let l = l.trim_start();
                let l = l.strip_prefix("> ").unwrap_or(l);
                l.strip_prefix("- **`")
                    .and_then(|rest| rest.strip_prefix(v))
                    .is_some_and(|after| !after.starts_with(|c: char| c.is_alphanumeric()))
            })
        };

        // RULE 1, and it applies to EVERY variant including the ones with a bullet. The
        // paragraph promises to enumerate the implemented kinds, so an omission is not a
        // gap in the docs — it is the page stating the kind does not exist.
        let unlisted: Vec<&str> = variants
            .iter()
            .copied()
            .filter(|v| !status.contains(&format!("`{v}`")))
            .collect();
        assert!(
            unlisted.is_empty(),
            "node kinds implemented but MISSING FROM THE ENUMERATION in \
             docs/features/orchestrator/execution-graph.md: {unlisted:?} — the \
             \"Implemented node kinds:\" paragraph is a CLOSED list and must name every \
             variant backticked (a bullet further down does not excuse the omission; see \
             this test's doc comment)"
        );

        // RULE 2, ADDITIONAL to rule 1 rather than an alternative to it: every kind added
        // since `Subgraph` also owns a bullet that says what it DOES.
        let undocumented: Vec<&str> = variants
            .iter()
            .copied()
            .filter(|&v| !GRANDFATHERED.contains(&v) && !has_own_bullet(v))
            .collect();
        assert!(
            undocumented.is_empty(),
            "node kinds implemented but not DOCUMENTED in docs/features/orchestrator/\
             execution-graph.md: {undocumented:?} — each needs its own \
             `> - **`Name {{ … }}`** — …` bullet (a mention in someone else's prose is not \
             documentation; see this test's doc comment)"
        );
    }
}
