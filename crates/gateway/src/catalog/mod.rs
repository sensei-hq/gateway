mod assemble;
mod presets;
mod tiers;
mod totals;
pub use assemble::*;
pub use presets::*;
pub use tiers::*;
pub use totals::*;

use crate::types::config::{CostBand, ModelConfig};

/// Blended $/1k cost band for a catalog model. An explicit `catalog.cost_band`
/// override wins; otherwise derive from `pricing` via `input_per_1k + output_per_1k`:
///   no pricing (or ≤ 0) ⇒ Free;  < 0.002 ⇒ Low;  < 0.02 ⇒ Mid;  else High.
/// Thresholds are documented in `docs/features/catalog/tiers-and-chains.md` and
/// boundary-tested.
pub fn cost_band(model: &ModelConfig) -> CostBand {
    if let Some(band) = model.catalog.as_ref().and_then(|c| c.cost_band) {
        return band;
    }
    match model.pricing.as_ref() {
        None => CostBand::Free,
        Some(p) => {
            let blended = p.input_per_1k + p.output_per_1k;
            if blended <= 0.0 {
                CostBand::Free
            } else if blended < 0.002 {
                CostBand::Low
            } else if blended < 0.02 {
                CostBand::Mid
            } else {
                CostBand::High
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::capability::Capability;
    use kernel::types::config::{CatalogMeta, CostBand, ModelConfig, ModelPricing};

    fn model_with_pricing(pricing: Option<ModelPricing>) -> ModelConfig {
        ModelConfig {
            id: "m".into(),
            api_model_id: None,
            provider: "p".into(),
            capabilities: vec![Capability::TextChat],
            context_window: 8000,
            max_output_tokens: 1000,
            pricing,
            family: None,
            catalog: None,
        }
    }

    #[test]
    fn cost_band_derives_from_pricing_override_wins() {
        assert_eq!(cost_band(&model_with_pricing(None)), CostBand::Free); // no pricing → Free
        assert_eq!(
            cost_band(&model_with_pricing(Some(ModelPricing {
                input_per_1k: 0.0002,
                output_per_1k: 0.0006,
                per_request: None
            }))),
            CostBand::Low
        ); // 0.0008 < 0.002
        assert_eq!(
            cost_band(&model_with_pricing(Some(ModelPricing {
                input_per_1k: 0.0008,
                output_per_1k: 0.004,
                per_request: None
            }))),
            CostBand::Mid
        ); // 0.0048 < 0.02
        assert_eq!(
            cost_band(&model_with_pricing(Some(ModelPricing {
                input_per_1k: 0.02,
                output_per_1k: 0.06,
                per_request: None
            }))),
            CostBand::High
        ); // 0.08 ≥ 0.02
        // Boundary values (strict `<`): exactly 0.002 → Mid, exactly 0.02 → High.
        assert_eq!(
            cost_band(&model_with_pricing(Some(ModelPricing {
                input_per_1k: 0.002,
                output_per_1k: 0.0,
                per_request: None
            }))),
            CostBand::Mid
        );
        assert_eq!(
            cost_band(&model_with_pricing(Some(ModelPricing {
                input_per_1k: 0.02,
                output_per_1k: 0.0,
                per_request: None
            }))),
            CostBand::High
        );
        // Explicit override wins.
        let mut over = model_with_pricing(Some(ModelPricing {
            input_per_1k: 0.02,
            output_per_1k: 0.06,
            per_request: None,
        })); // would be High
        over.catalog = Some(CatalogMeta {
            cost_band: Some(CostBand::Low),
            ..Default::default()
        });
        assert_eq!(cost_band(&over), CostBand::Low);
    }
}
