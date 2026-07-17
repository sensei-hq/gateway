use crate::types::config::ModelPricing;
use crate::types::cost::CostEstimate;

/// A model paired with its cost estimate and budget status.
#[derive(Debug, Clone)]
pub struct AffordableModel {
    pub model: String,
    pub cost_estimate: CostEstimate,
    pub within_budget: bool,
}

/// Result of partitioning models by budget affordability.
#[derive(Debug)]
pub struct BudgetFilterResult {
    pub affordable: Vec<AffordableModel>,
    pub over_budget: Vec<AffordableModel>,
}

/// Estimate the cost for a model given its pricing and token counts.
///
/// input_cost = input_tokens * pricing.input_per_1k / 1000
/// output_cost = max_output_tokens * pricing.output_per_1k / 1000
/// estimated = input_cost + output_cost
pub fn estimate_cost(
    pricing: &ModelPricing,
    model_id: &str,
    input_tokens: u32,
    max_output_tokens: u32,
) -> CostEstimate {
    let input_cost = input_tokens as f64 * pricing.input_per_1k / 1000.0;
    let output_cost = max_output_tokens as f64 * pricing.output_per_1k / 1000.0;
    let estimated = input_cost + output_cost;

    CostEstimate {
        estimated,
        minimum: input_cost,
        maximum: estimated,
        currency: "USD".to_string(),
        model: model_id.to_string(),
    }
}

/// Partition models into affordable (estimated <= budget) and over_budget.
pub fn filter_by_budget(models: &[(String, CostEstimate)], budget: f64) -> BudgetFilterResult {
    let mut affordable = Vec::new();
    let mut over_budget = Vec::new();

    for (model, estimate) in models {
        let within = estimate.estimated <= budget;
        let entry = AffordableModel {
            model: model.clone(),
            cost_estimate: estimate.clone(),
            within_budget: within,
        };
        if within {
            affordable.push(entry);
        } else {
            over_budget.push(entry);
        }
    }

    BudgetFilterResult {
        affordable,
        over_budget,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::ModelPricing;

    fn haiku_pricing() -> ModelPricing {
        ModelPricing {
            input_per_1k: 0.0008,
            output_per_1k: 0.004,
            per_request: None,
        }
    }

    fn sonnet_pricing() -> ModelPricing {
        ModelPricing {
            input_per_1k: 0.003,
            output_per_1k: 0.015,
            per_request: None,
        }
    }

    #[test]
    fn estimate_cost_haiku() {
        let est = estimate_cost(&haiku_pricing(), "claude-haiku", 1000, 500);

        // input: 1000 * 0.0008 / 1000 = 0.0008
        // output: 500 * 0.004 / 1000 = 0.002
        // estimated: 0.0028
        assert!((est.estimated - 0.0028).abs() < f64::EPSILON);
        assert_eq!(est.model, "claude-haiku");
        assert_eq!(est.currency, "USD");
    }

    #[test]
    fn estimate_cost_zero_tokens() {
        let est = estimate_cost(&haiku_pricing(), "claude-haiku", 0, 0);

        assert!((est.estimated - 0.0).abs() < f64::EPSILON);
        assert!((est.minimum - 0.0).abs() < f64::EPSILON);
        assert!((est.maximum - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn filter_separates_affordable_and_over() {
        let cheap = estimate_cost(&haiku_pricing(), "claude-haiku", 1000, 500);
        // cheap.estimated = 0.0028

        let expensive = estimate_cost(&sonnet_pricing(), "claude-sonnet", 1000, 500);
        // expensive: input 1000*0.003/1000=0.003, output 500*0.015/1000=0.0075
        // estimated = 0.0105

        let models = vec![
            ("claude-haiku".to_string(), cheap),
            ("claude-sonnet".to_string(), expensive),
        ];

        let result = filter_by_budget(&models, 0.01);

        assert_eq!(result.affordable.len(), 1);
        assert_eq!(result.affordable[0].model, "claude-haiku");
        assert!(result.affordable[0].within_budget);

        assert_eq!(result.over_budget.len(), 1);
        assert_eq!(result.over_budget[0].model, "claude-sonnet");
        assert!(!result.over_budget[0].within_budget);
    }

    #[test]
    fn filter_all_affordable() {
        let cheap = estimate_cost(&haiku_pricing(), "claude-haiku", 1000, 500);
        let medium = estimate_cost(&sonnet_pricing(), "claude-sonnet", 1000, 500);

        let models = vec![
            ("claude-haiku".to_string(), cheap),
            ("claude-sonnet".to_string(), medium),
        ];

        let result = filter_by_budget(&models, 1000.0);

        assert_eq!(result.affordable.len(), 2);
        assert!(result.over_budget.is_empty());
    }

    #[test]
    fn filter_empty_input() {
        let models: Vec<(String, CostEstimate)> = vec![];
        let result = filter_by_budget(&models, 1.0);

        assert!(result.affordable.is_empty());
        assert!(result.over_budget.is_empty());
    }
}
