use crate::selection::SelectedModel;

/// Orders admitted candidates. SP-0 ships PriorityStrategy (the single ordering
/// seam, replacing the hardcoded sort); tier/headroom strategies arrive in SP-CAT/SP-DATA.
pub trait RoutingStrategy: Send + Sync {
    fn order(&self, admitted: &mut Vec<SelectedModel>);
}

pub struct PriorityStrategy;
impl RoutingStrategy for PriorityStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>) {
        admitted.sort_by_key(|m| m.priority); // stable; identical to resolve_chain's sort today
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::{ModelConfig, RouterConfig};
    use std::collections::HashMap;

    // Build a minimal SelectedModel with the given model id + priority, mirroring
    // the field construction used in selection.rs's own tests.
    fn sm(model: &str, priority: u8) -> SelectedModel {
        let router_config = RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let model_config = ModelConfig {
            id: model.to_string(),
            api_model_id: None,
            provider: "test".to_string(),
            family: None,
            capabilities: vec![],
            context_window: 0,
            max_output_tokens: 0,
            pricing: None,
            catalog: None,
        };
        SelectedModel {
            model: model.to_string(),
            router: "test".to_string(),
            router_config,
            model_config,
            api_model_id: model.to_string(),
            priority,
            cost_estimate: None,
        }
    }

    #[test]
    fn priority_strategy_sorts_ascending_by_priority() {
        let mut v = vec![sm("b", 2), sm("a", 1)];
        PriorityStrategy.order(&mut v);
        assert_eq!(v[0].model, "a");
        assert_eq!(v[1].model, "b");
    }

    #[test]
    fn priority_strategy_is_stable_for_equal_priority() {
        let mut v = vec![sm("first", 1), sm("second", 1)];
        PriorityStrategy.order(&mut v);
        assert_eq!(v[0].model, "first"); // stable: equal keys keep input order
        assert_eq!(v[1].model, "second");
    }
}
