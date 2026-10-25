//! The category taxonomy: what a measurement is *evidence for*.
//!
//! A scenario is an experiment ("s1b", banking77); a category is the capability
//! a consumer orders by ("tool.route"). The mapping is bidirectional and fixed
//! here so the artifact, the gateway and torii agree on one vocabulary rather
//! than each deriving it from scenario names.

use crate::case::Scenario;

/// The capability a ranking is published for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Category {
    IntentRoute,
    ToolRoute,
    TriageScore,
    EntailmentCheck,
    GateTestOutput,
    GateReviewFinding,
    GateFixCheck,
    /// Which planner agent runs for a goal. Hand-curated cases; until they
    /// exist, [`Category::proxy`] says which measurement stands in.
    PlannerSelect,
}

impl Category {
    /// Every category, in the order an artifact lists them.
    pub const ALL: [Category; 8] = [
        Category::IntentRoute,
        Category::ToolRoute,
        Category::TriageScore,
        Category::EntailmentCheck,
        Category::GateTestOutput,
        Category::GateReviewFinding,
        Category::GateFixCheck,
        Category::PlannerSelect,
    ];

    /// The stable id in the artifact and in torii's `catalog.model_rankings`.
    pub fn id(self) -> &'static str {
        match self {
            Category::IntentRoute => "intent.route",
            Category::ToolRoute => "tool.route",
            Category::TriageScore => "triage.score",
            Category::EntailmentCheck => "entailment.check",
            Category::GateTestOutput => "gate.test_output",
            Category::GateReviewFinding => "gate.review_finding",
            Category::GateFixCheck => "gate.fix_check",
            Category::PlannerSelect => "planner.select",
        }
    }

    /// The scenario that evidences this category.
    pub fn scenario(self) -> Option<Scenario> {
        match self {
            Category::IntentRoute => Some(Scenario::S1aIntentOos),
            Category::ToolRoute => Some(Scenario::S1bBanking77),
            Category::TriageScore => Some(Scenario::S2Sentiment),
            Category::EntailmentCheck => Some(Scenario::S3Entailment),
            Category::GateTestOutput => Some(Scenario::S4TestOutput),
            Category::GateReviewFinding => Some(Scenario::S4ReviewFinding),
            Category::GateFixCheck => Some(Scenario::S4FixCheck),
            Category::PlannerSelect => Some(Scenario::PlannerSelect),
        }
    }

    /// The category whose measurement stands in for this one while it has no
    /// rows of its own. The artifact records it as `proxy`, so a consumer can
    /// tell a measurement from a stand-in.
    pub fn proxy(self) -> Option<Category> {
        match self {
            Category::PlannerSelect => Some(Category::ToolRoute),
            _ => None,
        }
    }

    /// Parse a category id. Unknown ids are `None`, never a silent default.
    pub fn from_id(id: &str) -> Option<Category> {
        Category::ALL.into_iter().find(|c| c.id() == id)
    }
}

/// The categories a scenario is evidence for (the reverse of
/// [`Category::scenario`]). A scenario with no category cannot be published.
pub fn categories_for(scenario: Scenario) -> Vec<Category> {
    Category::ALL
        .into_iter()
        .filter(|c| c.scenario() == Some(scenario))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip() {
        for c in Category::ALL {
            assert_eq!(Category::from_id(c.id()), Some(c), "{}", c.id());
        }
        assert_eq!(Category::from_id("planner"), None);
        assert_eq!(Category::from_id(""), None);
    }

    #[test]
    fn the_taxonomy_covers_every_scenario_exactly_once() {
        for s in Scenario::ALL {
            let cats = categories_for(s);
            assert_eq!(cats.len(), 1, "{s:?} must map to exactly one category");
            assert_eq!(cats[0].scenario(), Some(s));
        }
        // Every category names a distinct scenario, so no measurement is
        // published under two ids by accident.
        let mut seen = Vec::new();
        for c in Category::ALL {
            let s = c.scenario().unwrap();
            assert!(!seen.contains(&s), "{s:?} is claimed twice");
            seen.push(s);
        }
    }

    #[test]
    fn planner_select_has_its_own_cases_and_a_labelled_proxy() {
        let c = Category::PlannerSelect;
        assert_eq!(c.scenario(), Some(Scenario::PlannerSelect));
        assert_eq!(c.proxy(), Some(Category::ToolRoute));
        // A category with an own measurement is not a proxy of anything.
        assert_eq!(Category::ToolRoute.proxy(), None);
        assert_eq!(Category::ToolRoute.scenario(), Some(Scenario::S1bBanking77));
    }
}
