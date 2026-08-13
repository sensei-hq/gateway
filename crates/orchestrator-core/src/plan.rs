//! Plan types + the pure validator (SP-3 slice 4A). A planner emits a
//! `PlannedGraph` (the executable `Graph` + a `NodePlan` metadata side-map);
//! `parse_plan` + `feasible` are the deterministic gate used both by the planner's
//! `validate_plan` tool and by `run_expand` before journaling `PlanExpanded`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::graph::{Graph, NodeKind};
use crate::ids::NodeId;
use crate::registry::Registry;

/// Path segment reserved for the planner sub-run (`"{expand}/__plan__"`); a plan
/// node may not use it as its id (would collide once namespaced).
pub const RESERVED_PLAN_ID: &str = "__plan__";

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

/// Pure feasibility: structure (validate_dag) + registry-resolvable refs (Agent
/// nodes + each NodePlan.needs) + reserved-id + node-count. Returns ALL errors.
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
    for n in &plan.graph.nodes {
        if n.id.0 == RESERVED_PLAN_ID {
            errs.push(PlanError::ReservedNodeId(n.id.0.clone()));
        }
        if let NodeKind::Agent { agent, .. } = &n.kind
            && registry.agent(&agent.0).is_none()
        {
            errs.push(PlanError::UnknownAgent(agent.0.clone()));
        }
    }
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

    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Dep, Graph, Node, NodeKind};
    use crate::registry::{AgentDefinition, Registry};
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
}
