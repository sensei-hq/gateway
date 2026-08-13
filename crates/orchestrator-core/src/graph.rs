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
    /// `{ iterations, converged, output }`.
    Loop {
        body: MapBody,
        input: serde_json::Value,
        gate: LoopGate,
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
}

/// How an `Expand` node's plan is produced (SP-3 slice 4A). `Injected` = the
/// slice-3 `Planner` trait (deterministic/test); `Agent` = a journaled ReAct
/// planner agent (this slice). Slice 4B adds `Select` (goal-based selection).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum PlannerRef {
    Agent(crate::registry::AgentRef),
    #[default]
    Injected,
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

        // 2c. A `Subgraph`'s nested graph must itself be a valid DAG (recursive).
        for node in &self.nodes {
            if let NodeKind::Subgraph { graph } = &node.kind {
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

    #[test]
    fn validate_dag_rejects_a_zero_iteration_loop() {
        let graph = Graph {
            nodes: vec![Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: MapBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({}),
                    gate: LoopGate::TextContains("x".into()),
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
}
