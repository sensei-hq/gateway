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
}

/// The largest [`NodeKind::AwaitSignal`] timeout [`Graph::validate_dag`] accepts:
/// 100 Julian years.
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

/// A `Loop`'s stop decision (SP-3 s5). `Pure` = the SP-1 pure predicate (no journaling);
/// `Agent` = a gate-agent over the iteration output, then a pure `stop_when` over the
/// agent's answer (the agent turn is journaled ⇒ resume replays it).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GateSpec {
    Pure(LoopGate),
    Agent {
        agent: crate::registry::AgentRef,
        stop_when: LoopGate,
    },
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
}
