//! Plan types + the pure validator (SP-3 slice 4A). A planner emits a
//! `PlannedGraph` (the executable `Graph` + a `NodePlan` metadata side-map);
//! `parse_plan` + `feasible` are the deterministic gate used both by the planner's
//! `validate_plan` tool and by `run_expand` before journaling `PlanExpanded`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::graph::{Graph, LoopBody, MapBody, NodeKind};
use crate::ids::NodeId;
use crate::registry::Registry;

/// Path segment reserved for the planner sub-run (`"{expand}/__plan__"`); a plan
/// node may not use it as its id (would collide once namespaced).
pub const RESERVED_PLAN_ID: &str = "__plan__";

/// Path segment reserved for a Loop's gate-agent (`"{loop}/{i}/__gate__"`); a plan
/// node may not use it as its id (an Expand-body plan node would namespace to the
/// gate path and collide). Reserved for all plans (harmless for gate-less top-level
/// Expands) so `feasible` needs no path context.
pub const RESERVED_GATE_ID: &str = "__gate__";

/// Human-meaningful plan metadata for one node (viz + tracking + declared needs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct NodePlan {
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub needs: NodeNeeds,
}

/// A node's declared requirements — doubles as its planner-selected activation set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct NodeNeeds {
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub self_discover: bool,
}

/// What a planner emits: the executable graph + its per-node metadata (local ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedGraph {
    pub graph: Graph,
    #[serde(default)]
    pub node_plans: HashMap<NodeId, NodePlan>,
}

/// A feasibility/parse error over a produced plan.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    Parse(String),
    Structural(String),
    UnknownAgent(String),
    UnknownSkill(String),
    UnknownTool(String),
    ReservedNodeId(String),
    TooManyNodes { count: usize, limit: usize },
}

/// Parse planner output JSON into a `PlannedGraph`.
pub fn parse_plan(text: &str) -> Result<PlannedGraph, PlanError> {
    serde_json::from_str(text).map_err(|e| PlanError::Parse(e.to_string()))
}

/// Recursively check every agent ref against the registry: top-level `Agent`
/// nodes, the `MapBody::Agent` body of `Map`/`Consolidate` nodes, a `Loop`'s
/// `LoopBody::Agent` body (and, descending, a `LoopBody::Subgraph` body's graph),
/// and — mirroring `validate_dag`'s recursion — the same inside every `Subgraph`'s
/// graph and every `Branch`'s arm/`default` graphs. An unknown agent anywhere must
/// fail feasibility, not only at splice time (where it would `?`-propagate as a
/// fatal, non-resumable hard halt). `Expand` (top-level or a `LoopBody::Expand`
/// body) has no static nested graph (its plan is produced at runtime), so it is not
/// recursed into. Accumulates into `errs` (all errors, no short-circuit).
fn check_agent_refs(graph: &Graph, registry: &Registry, errs: &mut Vec<PlanError>) {
    for n in &graph.nodes {
        match &n.kind {
            NodeKind::Agent { agent, .. } => {
                if registry.agent(&agent.0).is_none() {
                    errs.push(PlanError::UnknownAgent(agent.0.clone()));
                }
            }
            NodeKind::Map { body, .. } | NodeKind::Consolidate { body, .. } => {
                if let MapBody::Agent(agent) = body
                    && registry.agent(&agent.0).is_none()
                {
                    errs.push(PlanError::UnknownAgent(agent.0.clone()));
                }
            }
            NodeKind::Loop { body, .. } => match body {
                LoopBody::Agent(agent) => {
                    if registry.agent(&agent.0).is_none() {
                        errs.push(PlanError::UnknownAgent(agent.0.clone()));
                    }
                }
                // A `Subgraph` body has a static nested graph — recurse (mirrors the
                // top-level `Subgraph` node). `ModelCall`/`Expand` have no static
                // agent ref to resolve here (`Expand`'s plan is checked at splice).
                LoopBody::Subgraph(g) => check_agent_refs(g, registry, errs),
                LoopBody::ModelCall { .. } | LoopBody::Expand { .. } => {}
            },
            NodeKind::Subgraph { graph } => check_agent_refs(graph, registry, errs),
            NodeKind::Branch { arms, default, .. } => {
                for (_, g) in arms {
                    check_agent_refs(g, registry, errs);
                }
                check_agent_refs(default, registry, errs);
            }
            _ => {}
        }
    }
}

/// Pure feasibility: structure (validate_dag) + registry-resolvable agent refs
/// (top-level `Agent` nodes, the `Map`/`Consolidate` `MapBody::Agent` bodies, a
/// `Loop`'s `LoopBody::Agent` body, and recursively through nested `Subgraph`/
/// `Branch` graphs plus a `LoopBody::Subgraph` body) + each `NodePlan.needs`
/// (agents/skills/tools) + reserved-id + node-count. Returns ALL errors.
pub fn feasible(
    plan: &PlannedGraph,
    registry: &Registry,
    max_nodes: usize,
) -> Result<(), Vec<PlanError>> {
    let mut errs = Vec::new();

    if let Err(e) = plan.graph.validate_dag() {
        errs.push(PlanError::Structural(e.to_string()));
    }
    if plan.graph.nodes.len() > max_nodes {
        errs.push(PlanError::TooManyNodes {
            count: plan.graph.nodes.len(),
            limit: max_nodes,
        });
    }
    // Reserved-id is a top-level-only pre-check: nested ids namespace deeper under
    // their parent path and can't collide with the top-level `__plan__` (planner
    // sub-run), `__gate__` (Loop gate-agent) or `__select__` (planner-selector spend)
    // segments.
    for n in &plan.graph.nodes {
        if n.id.0 == RESERVED_PLAN_ID
            || n.id.0 == RESERVED_GATE_ID
            || n.id.0 == crate::planner::RESERVED_SELECT_ID
        {
            errs.push(PlanError::ReservedNodeId(n.id.0.clone()));
        }
    }
    // Agent refs, recursing into nested Subgraph/Branch graphs.
    check_agent_refs(&plan.graph, registry, &mut errs);
    for np in plan.node_plans.values() {
        for a in &np.needs.agents {
            if registry.agent(a).is_none() {
                errs.push(PlanError::UnknownAgent(a.clone()));
            }
        }
        for s in &np.needs.skills {
            if registry.skill(s).is_none() {
                errs.push(PlanError::UnknownSkill(s.clone()));
            }
        }
        for t in &np.needs.tools {
            if registry.tool(t).is_none() {
                errs.push(PlanError::UnknownTool(t.clone()));
            }
        }
    }

    // Stable, process-independent error order — `ValidatePlan` is a Pure tool, so
    // its `{errors}` output must be deterministic (the node_plans HashMap and any
    // other accumulation source iterate in arbitrary order). `PlanError` has no
    // `Ord`; the debug-string key is a pragmatic total order over every variant.
    errs.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::EffectClass;
    use crate::graph::{Aggregation, BranchCond, Dep, Graph, MapBody, Node, NodeKind};
    use crate::registry::{
        Activation, AgentBacking, AgentDefinition, AgentRef, Permissions, Registry, SkillDef,
        ToolSpec,
    };
    use std::collections::HashMap;

    fn agent_reg() -> Registry {
        Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: HashMap::new(),
            grants: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "r".into(),
            backed_by: AgentBacking::Model,
        })
    }
    fn mc(id: &str, dep: Option<&str>) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::ModelCall {
                chain: "research.bulk".into(),
                payload: serde_json::json!({}),
            },
            deps: dep.map(|d| vec![Dep::hard(d)]).unwrap_or_default(),
        }
    }

    #[test]
    fn parse_plan_roundtrips_a_planned_graph() {
        let text = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"research.bulk","payload":{}}},"deps":[]}]},"node_plans":{"n1":{"label":"do it"}}}"#;
        let plan = parse_plan(text).expect("parses");
        assert_eq!(plan.graph.nodes.len(), 1);
        assert_eq!(plan.node_plans[&NodeId("n1".into())].label, "do it");
    }

    #[test]
    fn parse_plan_without_node_plans_defaults_empty() {
        let text = r#"{"graph":{"nodes":[]}}"#;
        let plan = parse_plan(text).expect("parses");
        assert!(plan.node_plans.is_empty());
    }

    #[test]
    fn parse_plan_rejects_malformed_json() {
        assert!(matches!(parse_plan("not json"), Err(PlanError::Parse(_))));
    }

    #[test]
    fn feasible_accepts_a_clean_plan() {
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![mc("n1", None)],
            },
            node_plans: HashMap::new(),
        };
        assert!(feasible(&plan, &agent_reg(), 512).is_ok());
    }

    #[test]
    fn feasible_reports_all_error_classes() {
        // dangling agent (via NodePlan.needs), reserved id, cycle, over-cap.
        let mut node_plans = HashMap::new();
        node_plans.insert(
            NodeId("n1".into()),
            NodePlan {
                label: "x".into(),
                description: None,
                needs: NodeNeeds {
                    agents: vec!["ghost".into()],
                    ..Default::default()
                },
            },
        );
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![
                    Node {
                        id: NodeId("__plan__".into()),
                        kind: mc("z", None).kind,
                        deps: vec![],
                    },
                    mc("n1", None),
                ],
            },
            node_plans,
        };
        let errs = feasible(&plan, &agent_reg(), 1).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::ReservedNodeId(_)))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::TooManyNodes { .. }))
        );
    }

    #[test]
    fn feasible_reports_a_reserved_gate_id() {
        // An untrusted Expand-body planner emitting a node id `"__gate__"` would
        // namespace to `"{loop}/{i}/__gate__"` and collide with the gate-agent path
        // (same effect_id → a later resume dies with DeterminismViolation). `feasible`
        // must reject it, exactly like the reserved `"__plan__"` segment.
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![Node {
                    id: NodeId("__gate__".into()),
                    kind: mc("z", None).kind,
                    deps: vec![],
                }],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::ReservedNodeId(id) if id == "__gate__"))
        );
    }

    #[test]
    fn feasible_reports_a_reserved_select_id() {
        // The third reserved segment. A selector's own model call is journaled under
        // `"{expand}/__select__"` so its spend reaches the ledger; an untrusted planner
        // emitting a node id `"__select__"` namespaces to exactly that path, collides
        // on effect_id, and kills the next resume with DeterminismViolation — the same
        // defect the s5 review found for `"__gate__"`.
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![Node {
                    id: NodeId("__select__".into()),
                    kind: mc("z", None).kind,
                    deps: vec![],
                }],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::ReservedNodeId(id) if id == "__select__"))
        );
    }

    #[test]
    fn feasible_reports_a_structural_cycle() {
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![mc("a", Some("b")), mc("b", Some("a"))],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, PlanError::Structural(_))));
    }

    /// An `Agent` node (used only inside nested graphs here) naming `agent`.
    fn agent_node(id: &str, agent: &str) -> Node {
        Node {
            id: NodeId(id.into()),
            kind: NodeKind::Agent {
                agent: AgentRef(agent.into()),
                input: serde_json::json!({}),
                phase: None,
            },
            deps: vec![],
        }
    }
    fn a_tool(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    #[test]
    fn feasible_reports_unknown_skill_and_tool_in_needs() {
        // node_plans[*].needs references an unregistered skill AND tool.
        let mut node_plans = HashMap::new();
        node_plans.insert(
            NodeId("n1".into()),
            NodePlan {
                label: "x".into(),
                description: None,
                needs: NodeNeeds {
                    skills: vec!["ghost_skill".into()],
                    tools: vec!["ghost_tool".into()],
                    ..Default::default()
                },
            },
        );
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![mc("n1", None)],
            },
            node_plans,
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownSkill(s) if s == "ghost_skill"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownTool(t) if t == "ghost_tool"))
        );
    }

    #[test]
    fn feasible_accepts_needs_referencing_registered_skill_and_tool() {
        // Positive path: needs resolve against a registry holding the skill + tool.
        let reg = agent_reg()
            .with_skill(SkillDef {
                name: "concise".into(),
                description: None,
                body: "b".into(),
                activation: Activation::default(),
            })
            .with_tool(a_tool("calc"));
        let mut node_plans = HashMap::new();
        node_plans.insert(
            NodeId("n1".into()),
            NodePlan {
                label: "x".into(),
                description: None,
                needs: NodeNeeds {
                    skills: vec!["concise".into()],
                    tools: vec!["calc".into()],
                    ..Default::default()
                },
            },
        );
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![mc("n1", None)],
            },
            node_plans,
        };
        assert!(feasible(&plan, &reg, 512).is_ok());
    }

    #[test]
    fn feasible_error_order_is_deterministic() {
        // Two unknown-agent needs (accumulated by iterating a HashMap) must render
        // in a stable order across repeated calls — the Pure-tool determinism prop.
        let mut node_plans = HashMap::new();
        node_plans.insert(
            NodeId("n1".into()),
            NodePlan {
                label: "x".into(),
                description: None,
                needs: NodeNeeds {
                    agents: vec!["ghost_a".into(), "ghost_b".into()],
                    ..Default::default()
                },
            },
        );
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![mc("n1", None)],
            },
            node_plans,
        };
        let first = feasible(&plan, &agent_reg(), 512).unwrap_err();
        for _ in 0..8 {
            assert_eq!(feasible(&plan, &agent_reg(), 512).unwrap_err(), first);
        }
    }

    #[test]
    fn feasible_recurses_agent_ref_check_into_nested_subgraph() {
        // A nested Subgraph's Agent{agent:"ghost"} must be caught (not only at runtime).
        let inner = Graph {
            nodes: vec![agent_node("a", "ghost")],
        };
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![Node {
                    id: NodeId("s".into()),
                    kind: NodeKind::Subgraph {
                        graph: Box::new(inner),
                    },
                    deps: vec![],
                }],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost"))
        );
    }

    #[test]
    fn feasible_reports_unknown_agent_in_a_map_body() {
        // A `Map` (also `Consolidate`/`Loop`) carries its agent in `body`, not as a
        // top-level `Agent` node — an unknown one must still be caught pre-splice.
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![Node {
                    id: NodeId("m".into()),
                    kind: NodeKind::Map {
                        body: MapBody::Agent(AgentRef("ghost".into())),
                        over: vec![serde_json::json!({})],
                        concurrency: 1,
                        aggregation: Aggregation::BestEffort,
                    },
                    deps: vec![],
                }],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost"))
        );
    }

    #[test]
    fn feasible_recurses_agent_ref_check_into_branch_arms_and_default() {
        // Both a Branch arm graph AND its default graph are descended into.
        let arm = Graph {
            nodes: vec![agent_node("arm", "ghost_arm")],
        };
        let default = Graph {
            nodes: vec![agent_node("def", "ghost_default")],
        };
        // validate_dag requires the Branch to Hard-depend on its `on` predecessor.
        let plan = PlannedGraph {
            graph: Graph {
                nodes: vec![
                    mc("p", None),
                    Node {
                        id: NodeId("b".into()),
                        kind: NodeKind::Branch {
                            on: NodeId("p".into()),
                            arms: vec![(BranchCond::FieldTrue("go".into()), arm)],
                            default,
                        },
                        deps: vec![Dep::hard("p")],
                    },
                ],
            },
            node_plans: HashMap::new(),
        };
        let errs = feasible(&plan, &agent_reg(), 512).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost_arm"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, PlanError::UnknownAgent(a) if a == "ghost_default"))
        );
    }
}
