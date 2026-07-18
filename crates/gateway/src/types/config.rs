use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::capability::Capability;

fn default_true() -> bool {
    true
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// Literal API key — populated by the caller (e.g. the daemon resolves
    /// from Keychain and inserts it here before passing the config to an
    /// adapter). Takes precedence over `api_key_env` when both are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Custom `Debug` that never prints the literal `api_key` — it renders as
/// `Some("***")` when set, `None` when absent. `RouterConfig` (and, transitively,
/// `GatewayConfig`) can otherwise leak the key into logs / error messages.
/// Note: operator-supplied `headers` are shown as-is; do not place secrets in
/// plain `headers` (use `api_key`/`api_key_env`).
impl std::fmt::Debug for RouterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterConfig")
            .field("url", &self.url)
            .field("api_key_env", &self.api_key_env)
            .field("api_key", &self.api_key.as_ref().map(|_| "***"))
            .field("enabled", &self.enabled)
            .field("timeout_ms", &self.timeout_ms)
            .field("headers", &self.headers)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input_per_1k: f64,
    pub output_per_1k: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_request: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_model_id: Option<String>,
    pub provider: String,
    pub capabilities: Vec<Capability>,
    pub context_window: u32,
    pub max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pricing: Option<ModelPricing>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackTrigger {
    RateLimit,
    Timeout,
    ProviderError,
    ModelUnavailable,
    BudgetExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainEntry {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub router: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_model_id: Option<String>,
    pub priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackChainConfig {
    pub id: String,
    pub capability: Capability,
    pub models: Vec<ChainEntry>,
    pub fallback_triggers: Vec<FallbackTrigger>,
}

// ---------------------------------------------------------------------------
// Subscription/quota constraints (AUTH). Operator-configured, provided at
// gateway init alongside routers/models/chains. Empty ⇒ no enforcement.
// See docs/design/subscription-quota-auth.md.
// ---------------------------------------------------------------------------

/// The unit a [`QuotaLimit`] is counted in. Dollars are integer milli-USD
/// (`cost_usd × 1000`) so quota counters stay integer; the f64 `Cost` USD path
/// is unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeterUnit {
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
    CostUsdMillis,
}

/// Rolling window a [`QuotaLimit`] applies over (start = `now − period`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Window {
    Day,
    Week,
    Month,
}

/// A single "at most `limit` of `unit` per `window`" cap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaLimit {
    pub unit: MeterUnit,
    pub window: Window,
    pub limit: u64,
}

/// The constraints for one subscription tier: limits that apply across all
/// modalities, plus optional per-capability (modality) additions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierConstraints {
    #[serde(default)]
    pub quota: Vec<QuotaLimit>,
    #[serde(default)]
    pub per_capability: HashMap<Capability, Vec<QuotaLimit>>,
}

/// Operator-configured constraint catalog. Empty ⇒ nothing is enforced
/// (today's behaviour). A request's `AuthContext.tier` selects a `TierConstraints`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConstraintsConfig {
    /// Per-tier constraint sets, keyed by tier label.
    #[serde(default)]
    pub tiers: HashMap<String, TierConstraints>,
    /// Applied when a request carries no tier, or a tier absent from `tiers`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<TierConstraints>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GatewayConfig {
    #[serde(default)]
    pub routers: HashMap<String, RouterConfig>,
    #[serde(default)]
    pub models: HashMap<String, ModelConfig>,
    #[serde(default)]
    pub chains: HashMap<String, FallbackChainConfig>,
    /// Subscription/quota constraints (AUTH). Default empty ⇒ no enforcement.
    #[serde(default)]
    pub constraints: ConstraintsConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_config_debug_redacts_api_key() {
        let cfg = RouterConfig {
            url: "https://x".into(),
            api_key_env: None,
            api_key: Some("sk-super-secret".into()),
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("sk-super-secret"),
            "api_key must not leak in Debug: {dbg}"
        );
        assert!(
            dbg.contains("***"),
            "Debug should mark the key as set: {dbg}"
        );
    }

    #[test]
    fn router_config_serde_roundtrip() {
        let config = RouterConfig {
            url: "https://api.openai.com/v1".to_string(),
            api_key_env: Some("OPENAI_API_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: Some(30000),
            headers: HashMap::from([("X-Custom".to_string(), "value".to_string())]),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RouterConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.url, "https://api.openai.com/v1");
        assert_eq!(deserialized.api_key_env, Some("OPENAI_API_KEY".to_string()));
        assert!(deserialized.enabled);
        assert_eq!(deserialized.timeout_ms, Some(30000));
        assert_eq!(deserialized.headers.get("X-Custom").unwrap(), "value");
    }

    #[test]
    fn router_config_defaults() {
        let json = r#"{"url": "https://api.example.com"}"#;
        let config: RouterConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.url, "https://api.example.com");
        assert!(config.enabled);
        assert!(config.api_key_env.is_none());
        assert!(config.api_key.is_none());
        assert!(config.timeout_ms.is_none());
        assert!(config.headers.is_empty());
    }

    #[test]
    fn model_config_serde_roundtrip() {
        let config = ModelConfig {
            id: "claude-sonnet".to_string(),
            api_model_id: Some("claude-3-5-sonnet-20241022".to_string()),
            provider: "anthropic".to_string(),
            capabilities: vec![Capability::TextChat, Capability::TextEmbed],
            context_window: 200000,
            max_output_tokens: 8192,
            pricing: Some(ModelPricing {
                input_per_1k: 0.003,
                output_per_1k: 0.015,
                per_request: None,
            }),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ModelConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "claude-sonnet");
        assert_eq!(deserialized.capabilities.len(), 2);
        assert_eq!(deserialized.capabilities[0], Capability::TextChat);
        assert_eq!(deserialized.capabilities[1], Capability::TextEmbed);
        assert!(deserialized.pricing.is_some());
    }

    #[test]
    fn fallback_chain_serde_roundtrip() {
        let chain = FallbackChainConfig {
            id: "chat-primary".to_string(),
            capability: Capability::TextChat,
            models: vec![
                ChainEntry {
                    model: "claude-sonnet".to_string(),
                    router: Some("anthropic".to_string()),
                    api_model_id: None,
                    priority: 1,
                },
                ChainEntry {
                    model: "gpt-4o".to_string(),
                    router: Some("openai".to_string()),
                    api_model_id: None,
                    priority: 2,
                },
            ],
            fallback_triggers: vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout],
        };

        let json = serde_json::to_string(&chain).unwrap();
        let deserialized: FallbackChainConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "chat-primary");
        assert_eq!(deserialized.models.len(), 2);
        assert_eq!(deserialized.models[0].priority, 1);
        assert_eq!(deserialized.fallback_triggers.len(), 2);
    }

    #[test]
    fn fallback_trigger_snake_case_serde() {
        let trigger = FallbackTrigger::RateLimit;
        let json = serde_json::to_string(&trigger).unwrap();
        assert_eq!(json, r#""rate_limit""#);

        let deserialized: FallbackTrigger = serde_json::from_str(r#""rate_limit""#).unwrap();
        assert_eq!(deserialized, FallbackTrigger::RateLimit);
    }

    #[test]
    fn gateway_config_default_is_empty() {
        let config = GatewayConfig::default();
        assert!(config.routers.is_empty());
        assert!(config.models.is_empty());
        assert!(config.chains.is_empty());
    }

    #[test]
    fn router_config_api_key_field_serializes() {
        let config = RouterConfig {
            url: "https://api.example.com".to_string(),
            api_key_env: None,
            api_key: Some("sk-literal".to_string()),
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"api_key\":\"sk-literal\""));

        let parsed: RouterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.api_key.as_deref(), Some("sk-literal"));
    }

    #[test]
    fn router_config_omits_api_key_when_none() {
        let config = RouterConfig {
            url: "https://api.example.com".to_string(),
            api_key_env: Some("X_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let json = serde_json::to_string(&config).unwrap();
        // "api_key_env" would contain "api_key" as a substring, so check the
        // exact JSON key form instead.
        assert!(!json.contains("\"api_key\":"));
    }

    #[test]
    fn constraints_default_when_absent() {
        // A config without a `constraints` key deserializes to an empty catalog
        // (⇒ no enforcement), so existing configs keep working unchanged.
        let json = r#"{"routers":{},"models":{},"chains":{}}"#;
        let cfg: GatewayConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.constraints.tiers.is_empty());
        assert!(cfg.constraints.default.is_none());
    }

    #[test]
    fn constraints_config_roundtrip_with_per_capability() {
        let mut tiers = HashMap::new();
        tiers.insert(
            "pro".to_string(),
            TierConstraints {
                quota: vec![
                    QuotaLimit {
                        unit: MeterUnit::Requests,
                        window: Window::Day,
                        limit: 1000,
                    },
                    QuotaLimit {
                        unit: MeterUnit::TotalTokens,
                        window: Window::Week,
                        limit: 1_000_000,
                    },
                ],
                per_capability: HashMap::from([(
                    Capability::ImageGenerate,
                    vec![QuotaLimit {
                        unit: MeterUnit::Requests,
                        window: Window::Day,
                        limit: 50,
                    }],
                )]),
            },
        );
        let cfg = GatewayConfig {
            constraints: ConstraintsConfig {
                tiers,
                default: None,
            },
            ..Default::default()
        };

        let json = serde_json::to_string(&cfg).unwrap();
        // Enum units/windows and the Capability map key serialize snake_case.
        assert!(json.contains("\"requests\"") && json.contains("\"day\""));
        assert!(json.contains("image_generate"));

        let back: GatewayConfig = serde_json::from_str(&json).unwrap();
        let pro = back.constraints.tiers.get("pro").expect("pro tier");
        assert_eq!(pro.quota.len(), 2);
        assert_eq!(pro.quota[0].unit, MeterUnit::Requests);
        assert_eq!(pro.quota[1].window, Window::Week);
        assert_eq!(pro.per_capability[&Capability::ImageGenerate][0].limit, 50);
    }
}
