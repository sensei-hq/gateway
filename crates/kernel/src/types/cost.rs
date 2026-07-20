use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cost {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
    pub input_cost: f64,
    pub output_cost: f64,
    pub total_cost: f64,
    pub currency: String,
}

impl Cost {
    pub fn zero() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            input_cost: 0.0,
            output_cost: 0.0,
            total_cost: 0.0,
            currency: "USD".to_string(),
        }
    }

    pub fn from_usage(usage: &TokenUsage, input_per_1k: f64, output_per_1k: f64) -> Self {
        let input_cost = (usage.input_tokens as f64 / 1000.0) * input_per_1k;
        let output_cost = (usage.output_tokens as f64 / 1000.0) * output_per_1k;
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
            input_cost,
            output_cost,
            total_cost: input_cost + output_cost,
            currency: "USD".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    pub estimated: f64,
    pub minimum: f64,
    pub maximum: f64,
    pub currency: String,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budget {
    pub daily_limit: f64,
    pub monthly_limit: f64,
    pub alert_threshold: f32,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            daily_limit: 5.0,
            monthly_limit: 50.0,
            alert_threshold: 0.8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_zero() {
        let cost = Cost::zero();
        assert_eq!(cost.total_cost, 0.0);
        assert_eq!(cost.currency, "USD");
        assert_eq!(cost.input_tokens, 0);
        assert_eq!(cost.output_tokens, 0);
        assert_eq!(cost.total_tokens, 0);
    }

    #[test]
    fn cost_from_usage() {
        let usage = TokenUsage {
            input_tokens: 1000,
            output_tokens: 500,
            total_tokens: 1500,
        };
        // Haiku pricing
        let cost = Cost::from_usage(&usage, 0.0008, 0.004);

        assert_eq!(cost.input_tokens, 1000);
        assert_eq!(cost.output_tokens, 500);
        assert_eq!(cost.total_tokens, 1500);
        assert!((cost.input_cost - 0.0008).abs() < f64::EPSILON);
        assert!((cost.output_cost - 0.002).abs() < f64::EPSILON);
        assert!((cost.total_cost - 0.0028).abs() < f64::EPSILON);
        assert_eq!(cost.currency, "USD");
    }

    #[test]
    fn cost_from_usage_zero_tokens() {
        let usage = TokenUsage::default();
        let cost = Cost::from_usage(&usage, 0.003, 0.015);

        assert_eq!(cost.input_tokens, 0);
        assert_eq!(cost.output_tokens, 0);
        assert_eq!(cost.total_tokens, 0);
        assert_eq!(cost.input_cost, 0.0);
        assert_eq!(cost.output_cost, 0.0);
        assert_eq!(cost.total_cost, 0.0);
    }

    #[test]
    fn budget_default() {
        let budget = Budget::default();
        assert_eq!(budget.daily_limit, 5.0);
        assert_eq!(budget.monthly_limit, 50.0);
        assert_eq!(budget.alert_threshold, 0.8);
    }

    #[test]
    fn cost_estimate_serde_roundtrip() {
        let estimate = CostEstimate {
            estimated: 0.05,
            minimum: 0.01,
            maximum: 0.10,
            currency: "USD".to_string(),
            model: "claude-sonnet".to_string(),
        };

        let json = serde_json::to_string(&estimate).unwrap();
        let deserialized: CostEstimate = serde_json::from_str(&json).unwrap();

        assert!((deserialized.estimated - 0.05).abs() < f64::EPSILON);
        assert!((deserialized.minimum - 0.01).abs() < f64::EPSILON);
        assert!((deserialized.maximum - 0.10).abs() < f64::EPSILON);
        assert_eq!(deserialized.currency, "USD");
        assert_eq!(deserialized.model, "claude-sonnet");
    }
}
