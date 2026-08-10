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

/// Optional catalog metadata for a model: free-tier terms plus attribute tags
/// (auth mechanism, cost band, locality). Absent ⇒ `None` ⇒ today's behaviour
/// (no free-tier accounting, no attribute-derived tiering). Pure config; not
/// persisted. See `docs/superpowers/specs/2026-08-07-sp-cat-catalog-design.md`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CatalogMeta {
    /// Free-tier terms, when the model offers a free allowance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free: Option<FreeTier>,
    /// How the model is authenticated (API key, OAuth CLI, keyless).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<AuthType>,
    /// Explicit cost band; when absent it is derived from `pricing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_band: Option<CostBand>,
    /// Where the model runs (local vs cloud).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locality: Option<Locality>,
    /// Arbitrary attribute labels (e.g. `"reasoning"`, `"frontier"`) used by
    /// tier predicates to derive membership on labels beyond the fixed
    /// attributes above. Absent ⇒ `[]` (backward-compatible; no tag-derived
    /// tiering).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Free-tier terms for a model: the recurrence shape, any token allowances, an
/// optional shared-pool key (models that draw from one provider allowance
/// dedupe by this key), ToS clarity, and whether prompts train the provider.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FreeTier {
    pub free_type: FreeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monthly_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credit_tokens: Option<u64>,
    /// Shared-allowance key: models sharing a provider free pool carry the same
    /// `pool_key` so budget audits count the pool once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_key: Option<String>,
    pub tos: TosVerdict,
    #[serde(default)]
    pub trains_on_prompts: bool,
}

/// The recurrence/shape of a free allowance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FreeType {
    RecurringDaily,
    RecurringMonthly,
    RecurringCredit,
    RecurringUncapped,
    OneTimeInitial,
    Keyless,
    Discontinued,
}

/// How clear the provider's terms of service are about free-tier use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TosVerdict {
    Ok,
    Caution,
    Ambiguous,
}

/// How a model is authenticated.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthType {
    ApiKey,
    OauthCli,
    Keyless,
}

/// Coarse blended cost band (derived from pricing unless overridden).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CostBand {
    Free,
    Low,
    Mid,
    High,
}

/// Where the model executes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Locality {
    Local,
    Cloud,
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
    /// Model lineage for panel distinctness (e.g. `"gemma"`, `"qwen"`,
    /// `"claude"`). Operator-declared and distinct from `provider` (the
    /// backend): `gemma4` and `qwen3` share `provider = "ollama"` but differ in
    /// family. `None` ⇒ the model id is treated as its own family for
    /// `distinct_by: family` panel checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// Optional catalog metadata (free-tier terms + attribute tags). Absent ⇒
    /// `None` ⇒ today's behaviour; kept serde-defaulted so existing configs
    /// deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogMeta>,
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

/// How a [`PanelConfig`] requires its member slots to differ, so a fan-out
/// panel actually queries *distinct* experts rather than collapsing onto one
/// model. See `Gateway::execute_panel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistinctBy {
    /// No distinctness requirement (e.g. a best-of-N / judge-selects panel).
    #[default]
    None,
    /// Slot primaries must be distinct model ids.
    Model,
    /// Slot primaries must be distinct model *families* (e.g. `gemma` vs `qwen`)
    /// — the axis that matters for consensus independence. See
    /// [`ModelConfig::family`].
    Family,
}

/// One member of a [`PanelConfig`]: a reference to an existing fallback chain,
/// so the slot inherits that chain's primary model plus its fallback legs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelSlot {
    /// Name of a chain in [`GatewayConfig::chains`] to run for this slot.
    pub chain: String,
    /// Human role label (e.g. `"proposer"`, `"challenger"`, `"reviewer"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// System prompt applied to **this slot only**, layered after any base
    /// request system prompt — so each debater can take a distinct persona
    /// (e.g. `"argue in favour"` vs `"red-team the proposal"`) instead of every
    /// slot receiving the identical request. Mirrors [`RoleSpec::system_prompt`]
    /// for the synthesizer/judge. No effect on non-chat capabilities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
}

/// A fan-out "panel": N slots (each a chain) run concurrently for one request,
/// returning all N answers for downstream aggregation (consensus / ensemble /
/// best-of-N). See `Gateway::execute_panel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelConfig {
    pub id: String,
    pub capability: Capability,
    pub slots: Vec<PanelSlot>,
    /// Distinctness enforced at formation time, before any inference.
    #[serde(default)]
    pub distinct_by: DistinctBy,
    /// When `true`, also enforce `distinct_by` at **runtime** (gh#21): after
    /// fan-out, a successful slot whose family a prior slot already produced is
    /// **dropped** (its result becomes an error) rather than returned, so no two
    /// returned slots share a family even when per-slot fallback converges.
    /// Default `false` — non-strict: keep both and only record the overlap in
    /// [`PanelResponse::collisions`]. No effect when `distinct_by` is `None`.
    #[serde(default)]
    pub strict: bool,
}

/// One non-fan-out role in a consensus workflow (synthesizer or judge): the
/// chain that runs it plus an optional system prompt shaping the role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleSpec {
    /// Name of a chain in [`GatewayConfig::chains`] to run this role.
    pub chain: String,
    /// System prompt prepended for this role (e.g. "Merge the proposals…").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
}

/// A consensus workflow: a fan-out debate ([`PanelConfig`]) whose outputs are
/// merged by a `synthesizer` and optionally evaluated by an independent
/// `judge`. See `Gateway::execute_consensus`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub id: String,
    pub capability: Capability,
    /// The debating members (fan-out), family-distinct via the panel's `distinct_by`.
    pub panel: PanelConfig,
    /// Merges the debaters' outputs into one answer.
    pub synthesizer: RoleSpec,
    /// Optional single final evaluator; must be family-independent of every
    /// debater (enforced before any inference). Mutually exclusive with
    /// `judge_quorum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<RoleSpec>,
    /// Optional final evaluator **quorum** (gh#20): a family-distinct panel of
    /// judges that each score the synthesis, for a vote/tally instead of one
    /// judge. Mutually exclusive with `judge`; every quorum member must also be
    /// family-independent of every debater. Per-judge personas come from each
    /// slot's [`PanelSlot::system_prompt`]. `None` ⇒ no quorum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_quorum: Option<PanelConfig>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Named fan-out panels, addressable by id via [`crate::types::request::InferenceRequest::panel`]
    /// and `Gateway::execute_panel_addressed`. Default empty (gh#19).
    #[serde(default)]
    pub panels: HashMap<String, PanelConfig>,
    /// Named consensus workflows, addressable by id via
    /// [`crate::types::request::InferenceRequest::consensus`] and
    /// `Gateway::execute_consensus_addressed`. Default empty (gh#19).
    #[serde(default)]
    pub consensus: HashMap<String, ConsensusConfig>,
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
    fn model_config_catalog_defaults_absent_and_roundtrips() {
        let json = r#"{"id":"m","provider":"p","capabilities":["text_chat"],"context_window":8000,"max_output_tokens":1000}"#;
        let m: ModelConfig = serde_json::from_str(json).unwrap();
        assert!(m.catalog.is_none()); // backward-compatible: existing configs deserialize unchanged
        let meta = CatalogMeta {
            free: Some(FreeTier {
                free_type: FreeType::RecurringDaily,
                monthly_tokens: Some(60_000_000),
                credit_tokens: None,
                pool_key: Some("gemini-flash".into()),
                tos: TosVerdict::Ok,
                trains_on_prompts: true,
            }),
            auth_type: Some(AuthType::ApiKey),
            cost_band: None,
            locality: Some(Locality::Cloud),
            tags: vec![],
        };
        let m2 = ModelConfig {
            catalog: Some(meta),
            ..m.clone()
        };
        let s = serde_json::to_string(&m2).unwrap();
        let back: ModelConfig = serde_json::from_str(&s).unwrap();
        let f = back.catalog.as_ref().unwrap().free.as_ref().unwrap();
        assert_eq!(f.pool_key.as_deref(), Some("gemini-flash"));
        assert!(matches!(f.free_type, FreeType::RecurringDaily));
        assert_eq!(
            back.catalog.as_ref().unwrap().auth_type,
            Some(AuthType::ApiKey)
        );
    }

    #[test]
    fn catalog_meta_tags_default_absent_and_roundtrip() {
        // Backward-compatible: a CatalogMeta JSON without `tags` deserializes to
        // an empty tag list (absent ⇒ []), so existing catalogs keep working.
        let meta: CatalogMeta = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(meta.tags, Vec::<String>::new());

        // A JSON carrying tags roundtrips faithfully.
        let meta: CatalogMeta = serde_json::from_str(r#"{"tags":["reasoning"]}"#).unwrap();
        assert_eq!(meta.tags, vec!["reasoning".to_string()]);
        let s = serde_json::to_string(&meta).unwrap();
        let back: CatalogMeta = serde_json::from_str(&s).unwrap();
        assert_eq!(back.tags, vec!["reasoning".to_string()]);
    }

    #[test]
    fn model_config_serde_roundtrip() {
        let config = ModelConfig {
            id: "claude-sonnet".to_string(),
            api_model_id: Some("claude-3-5-sonnet-20241022".to_string()),
            provider: "anthropic".to_string(),
            family: Some("claude".to_string()),
            capabilities: vec![Capability::TextChat, Capability::TextEmbed],
            context_window: 200000,
            max_output_tokens: 8192,
            pricing: Some(ModelPricing {
                input_per_1k: 0.003,
                output_per_1k: 0.015,
                per_request: None,
            }),
            catalog: None,
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
    fn panels_and_consensus_default_when_absent() {
        // A config without `panels`/`consensus` keys deserializes to empty maps,
        // so existing configs keep working unchanged (gh#19).
        let json = r#"{"routers":{},"models":{},"chains":{}}"#;
        let cfg: GatewayConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.panels.is_empty());
        assert!(cfg.consensus.is_empty());
    }

    #[test]
    fn panels_and_consensus_roundtrip() {
        let mut cfg = GatewayConfig::default();
        cfg.panels.insert(
            "board".to_string(),
            PanelConfig {
                id: "board".to_string(),
                capability: Capability::TextChat,
                slots: vec![
                    PanelSlot {
                        chain: "a".to_string(),
                        label: Some("proposer".to_string()),
                        system_prompt: Some("argue for".to_string()),
                    },
                    PanelSlot {
                        chain: "b".to_string(),
                        label: None,
                        system_prompt: None,
                    },
                ],
                distinct_by: DistinctBy::Family,
                strict: false,
            },
        );
        cfg.consensus.insert(
            "debate".to_string(),
            ConsensusConfig {
                id: "debate".to_string(),
                capability: Capability::TextChat,
                panel: PanelConfig {
                    id: "debate-panel".to_string(),
                    capability: Capability::TextChat,
                    slots: vec![PanelSlot {
                        chain: "a".to_string(),
                        label: None,
                        system_prompt: None,
                    }],
                    distinct_by: DistinctBy::Family,
                    strict: false,
                },
                synthesizer: RoleSpec {
                    chain: "s".to_string(),
                    system_prompt: Some("merge".to_string()),
                },
                judge: Some(RoleSpec {
                    chain: "j".to_string(),
                    system_prompt: None,
                }),
                judge_quorum: None,
            },
        );

        let json = serde_json::to_string(&cfg).unwrap();
        let back: GatewayConfig = serde_json::from_str(&json).unwrap();

        let board = back.panels.get("board").expect("board panel");
        assert_eq!(board.slots.len(), 2);
        assert_eq!(board.distinct_by, DistinctBy::Family);
        assert_eq!(board.slots[0].system_prompt.as_deref(), Some("argue for"));

        let debate = back.consensus.get("debate").expect("debate consensus");
        assert_eq!(debate.panel.slots.len(), 1);
        assert_eq!(debate.synthesizer.system_prompt.as_deref(), Some("merge"));
        assert!(debate.judge.is_some());
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
